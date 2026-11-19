// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A Postgres `real` (float4) column served from a stored table equals live,
 * value and JSON form, on both tiers (database and `storage=`), through every
 * kind of stored table: `#(materialize)`, `#@ persist` on a source, a view
 * and a query, and a `#(rollup)`.
 *
 * Live reads a Postgres row through JSON, and Postgres writes a float4 as its
 * shortest text, so a `real` holding 0.1 answers `0.1`. A `storage=` build
 * reads it through DuckDB's Postgres passthrough as a FLOAT, which a DuckDB
 * read gives as the double it widens to, `0.10000000149011612`. A sum of
 * `real` is a float4 in Postgres too; an avg is a float8, the widened values'
 * mean, which every tier already gave alike, and so is every number of a nest
 * (Malloy's Postgres dialect casts them to DOUBLE PRECISION).
 *
 * Every comparison here is exact (`JSON.stringify` of the rows), not R4's
 * float tolerance, which reads 0.1 and 0.10000000149011612 as equal.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL to a server the test may create
 * and drop databases on, e.g. `postgresql://postgres:pw@127.0.0.1:5432/postgres`.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
} from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../../src/service/access_record";
import { Environment } from "../../../src/service/environment";
import { parsePackageName } from "../../../src/service/identity";
import type { MaterializationService } from "../../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import type {
   Materialization,
   ResourceRepository,
} from "../../../src/storage/DatabaseInterface";
import type { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import { float4AsLive } from "../../../src/service/materialization/domain/live_float4";
import { freshLoadPool } from "../../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";
import {
   disposeTempStorage,
   openTempStorage,
} from "../../../src/test_helpers/temp_storage";

const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";

setDefaultTimeout(600_000);

/**
 * float4 values whose widened double's text is not their own: 0.1 widens to
 * 0.10000000149011612, 3.14159 to 3.141590118408203. Group `a` sums to a
 * float4 whose text is short (0.3 + 0.1 = 0.4) while its double sum is not.
 */
const SEED = [
   `CREATE TABLE f (id integer PRIMARY KEY, g text, r real, d double precision)`,
   `INSERT INTO f VALUES
      (1, 'a', 0.1, 0.1),
      (2, 'a', 0.3, 0.3),
      (3, 'b', 3.14159, 3.14159),
      (4, 'b', -2.7, -2.7),
      (5, 'c', 1.5, 1.5),
      (6, 'c', NULL, NULL),
      (7, 'c', 16777217, 16777217),
      (8, 'b', 1e-7, 1e-7),
      (9, 'a', 3.4e38, 1e300),
      (10, 'd', 1e7, 1e7),
      (11, 'd', 0.1, 0.1)`,
   // A join's side: a sum through it Malloy writes symmetrically, as numeric.
   `CREATE TABLE fb (id integer PRIMARY KEY, g text, r real)`,
   `INSERT INTO fb VALUES (1, 'x', 1e7), (2, 'x', 0.1)`,
];

/** The model, its stored tables in `storage` (`""` for the database tier). */
function files(storage: string): Record<string, string> {
   const lake = storage ? ` ${storage}` : "";
   return {
      "raw.malloy": `source: f_raw is pg.table('public.f') extend { primary_key: id }
`,
      "mat.malloy": `import "raw.malloy"

#(materialize)${lake}
source: fm is pg.table('public.f') extend {
  primary_key: id
  dimension: r_neg is -r
  measure: r_total is r.sum()
}

#(materialize)${lake}
source: fbig is pg.table('public.fb') extend { primary_key: id }

source: fmb is fm extend { join_one: o is fbig on o.id = id }

source: fmk is fm extend { join_many: k is fbig on k.g = g }
`,
      "persist.malloy": `##! experimental { persistence }
import "raw.malloy"

#@ persist name="f4_p"${lake}
source: fp is f_raw -> { select: * }

source: fv is f_raw extend {
  #@ persist name="f4_v"${lake}
  view: by_g is {
    group_by: g
    aggregate: tot is r.sum(), lo is r.min(), hi is r.max(), n is count()
  }
}

#@ persist name="f4_q"${lake}
query: fq is f_raw -> {
  group_by: g
  aggregate: tot is r.sum(), lo is r.min(), hi is r.max()
}
`,
      "rollup.malloy": `import "raw.malloy"

source: fr is f_raw extend {
  #(rollup) grain=["g"]${lake}
  measure: rsum is r.sum()
  #(rollup) grain=["g"]${lake}
  measure: rlo is r.min()
  #(rollup) grain=["g"]${lake}
  measure: rhi is r.max()
}
`,
   };
}

interface Query {
   /**
    * Columns live answers as a float8 or numeric that the stored answer
    * computes in DuckDB a little differently (a symmetric sum, which live
    * takes through NUMERIC; arithmetic over a float4 sum): never written as
    * a float4. Compared within 1e-6, and checked not to have been shortened
    * to a float4's text, where live's value is not one.
    */
   widenedOnly?: string[];
   name: string;
   model: string;
   malloy: string;
   /** How the served answer is answered: from a table, or from a rollup. */
   servedBy: "materialized" | "rollup" | "view" | "query";
}

const SELECT = "select: id, g, r, d; order_by: id";
const AGG = `group_by: g
  aggregate:
    tot is r.sum(), mean is r.avg(), lo is r.min(), hi is r.max()
  order_by: g`;

const QUERIES: Query[] = [
   ...[
      ["#(materialize)", "mat.malloy", "fm"],
      ["#@ persist source", "persist.malloy", "fp"],
   ].flatMap(([label, model, source]) => [
      {
         name: `${label}: select`,
         model,
         malloy: `run: ${source} -> { ${SELECT} }`,
         servedBy: "materialized" as const,
      },
      {
         name: `${label}: sum, avg, min and max by group`,
         model,
         malloy: `run: ${source} -> { ${AGG} }`,
         servedBy: "materialized" as const,
      },
      {
         name: `${label}: group by the real column`,
         model,
         malloy: `run: ${source} -> { group_by: r; aggregate: n is count(); order_by: r }`,
         servedBy: "materialized" as const,
      },
      {
         name: `${label}: a filter on it`,
         model,
         malloy: `run: ${source} -> { where: r > 0.2; select: id, r; order_by: id }`,
         servedBy: "materialized" as const,
      },
   ]),
   {
      name: "#(materialize): a dimension and a measure the source declares",
      model: "mat.malloy",
      malloy:
         "run: fm -> { group_by: g; aggregate: r_total, lo is r_neg.min(); order_by: g }",
      servedBy: "materialized",
   },
   {
      name: "#(materialize): a nest",
      model: "mat.malloy",
      malloy:
         "run: fm -> { group_by: g; nest: rows is { select: id, r; order_by: id }; order_by: g }",
      servedBy: "materialized",
   },
   {
      name: "#(materialize): a sum through a join_one, symmetric and numeric live",
      model: "mat.malloy",
      malloy: "run: fmb -> { aggregate: x is o.r.sum(), hi is o.r.max() }",
      widenedOnly: ["x"],
      servedBy: "materialized",
   },
   {
      name: "#(materialize): a base sum in a query that also reads a join_many",
      model: "mat.malloy",
      malloy:
         "run: fmk -> { where: g != 'c'; group_by: g; aggregate: s is r.sum(), hi is r.max(), n is k.count(); order_by: g }",
      widenedOnly: ["s"],
      servedBy: "materialized",
   },
   {
      name: "#(materialize): a division of reals, a float8 live",
      model: "mat.malloy",
      malloy:
         "run: fm -> { group_by: g; aggregate: ratio is r.sum() / r.max(), share is r.sum() / all(r.sum()); order_by: g }",
      widenedOnly: ["ratio", "share"],
      servedBy: "materialized",
   },
   {
      name: "#(materialize): a pick of reals and a coalesce with a literal",
      model: "mat.malloy",
      malloy:
         "run: fm -> { select: id, p is pick r when r > 1 else r_neg, q is pick r when r > 1 else 0.5, c is r ?? 0.5; order_by: id }",
      servedBy: "materialized",
   },
   {
      name: "#@ persist view",
      model: "persist.malloy",
      malloy: "run: fv -> by_g",
      servedBy: "view",
   },
   {
      name: "#@ persist query",
      model: "persist.malloy",
      malloy: "run: fq",
      servedBy: "query",
   },
   {
      name: "#(rollup) sum, min and max",
      model: "rollup.malloy",
      malloy:
         "run: fr -> { group_by: g; aggregate: rsum, rlo, rhi; order_by: g }",
      servedBy: "rollup",
   },
   {
      name: "#(rollup) rolled up past its grain",
      model: "rollup.malloy",
      malloy: "run: fr -> { aggregate: rsum, rlo, rhi }",
      servedBy: "rollup",
   },
];

const TIERS = [
   { label: "database", storage: "", servedFrom: "database" },
   {
      label: "storage= (DuckLake)",
      storage: 'storage="lake"',
      servedFrom: "storage",
   },
];

interface Answer {
   json: string;
   servedBy?: string;
   servedFrom: string | null;
}

type Outcome = { answer: Answer } | { error: string };

describe.skipIf(!POSTGRES_URL)(
   "a Postgres real column served equals live",
   () => {
      for (const tier of TIERS) {
         describe(`[${tier.label}]`, () => {
            const token = Math.random().toString(36).slice(2, 8);
            const warehouse = `float4_wh_${token}`;
            const catalog = `float4_lake_${token}`;
            let admin: PostgresConnection;
            let root: string;
            let db: DuckDBConnection;
            let repo: ResourceRepository;
            let env: Environment | undefined;
            let service: MaterializationService;
            let restore = () => {};
            let run: Materialization | undefined;
            const live = new Map<Query, Outcome>();
            const served = new Map<Query, Outcome>();

            async function ask(q: Query): Promise<Outcome> {
               const pkg = await env!.getPackage(parsePackageName(PKG), false);
               const model = pkg.getModel(q.model);
               if (!model) return { error: `${q.model} did not load` };
               try {
                  const result = await model.runQuery(
                     unrecordedAudit(),
                     { query: q.malloy } as never,
                     QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
                  );
                  return {
                     answer: {
                        json: JSON.stringify(result.compactResult),
                        servedBy: result.path?.servedBy?.kind,
                        servedFrom: result.servedFrom ?? null,
                     },
                  };
               } catch (error) {
                  return {
                     error:
                        error instanceof Error ? error.message : String(error),
                  };
               }
            }

            async function build(): Promise<Materialization> {
               const created = await service.createMaterialization(
                  ENV,
                  PKG,
                  {},
               );
               const running = (
                  service as unknown as {
                     runningAbortControllers: Map<string, unknown>;
                  }
               ).runningAbortControllers;
               const deadline = Date.now() + 300_000;
               for (;;) {
                  const m = await repo.getMaterializationById(created.id);
                  if (
                     m &&
                     ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(
                        m.status,
                     )
                  ) {
                     while (running.has(created.id)) {
                        await new Promise((resolve) => setTimeout(resolve, 10));
                     }
                     return (
                        (await repo.getMaterializationById(created.id)) ?? m
                     );
                  }
                  if (Date.now() > deadline)
                     throw new Error("run never settled");
                  await new Promise((resolve) => setTimeout(resolve, 50));
               }
            }

            beforeAll(async () => {
               restore = installSettings({
                  env: {
                     PUBLISHER_MATERIALIZE: "true",
                     PERSIST_STORAGE_MODE: "on",
                  },
               }).restore;
               await freshLoadPool();
               ({ root, db, repo } = await openTempStorage("float4-pg-", {
                  id: ENV_ID,
                  name: ENV,
               }));
               admin = new PostgresConnection({
                  name: "admin",
                  connectionString: POSTGRES_URL!,
               });
               await admin.runSQL(`CREATE DATABASE ${warehouse}`);
               if (tier.storage)
                  await admin.runSQL(`CREATE DATABASE ${catalog}`);
               const parsed = new URL(POSTGRES_URL!);
               const pg = {
                  host: parsed.hostname,
                  port: Number(parsed.port || 5432),
                  userName: decodeURIComponent(parsed.username),
                  password: decodeURIComponent(parsed.password),
               };
               const seed = new PostgresConnection({
                  name: "seed",
                  connectionString: `${parsed.protocol}//${parsed.username}:${parsed.password}@${parsed.host}/${warehouse}`,
               });
               for (const sql of SEED) await seed.runSQL(sql);
               await seed.close();

               const envPath = path.join(root, "env");
               writePackageFiles(path.join(envPath, PKG), files(tier.storage), {
                  manifest: { name: PKG, description: "float4 parity" },
               });
               const bucket = path.join(root, "bucket");
               fs.mkdirSync(bucket);
               env = await Environment.create(
                  ENV,
                  envPath,
                  [
                     {
                        name: "pg",
                        type: "postgres",
                        postgresConnection: { ...pg, databaseName: warehouse },
                     } as never,
                  ],
                  tier.storage
                     ? [
                          {
                             name: "lake",
                             type: "ducklake",
                             ducklakeConnection: {
                                catalog: {
                                   postgresConnection: {
                                      ...pg,
                                      databaseName: catalog,
                                   },
                                },
                                storage: { bucketUrl: bucket },
                             },
                          } as never,
                       ]
                     : [],
               );
               await env.addPackage(PKG);
               service = serviceOverEnvironment(
                  () => repo,
                  () => env!,
               );
               for (const q of QUERIES) live.set(q, await ask(q));
               run = await build();
               for (const q of QUERIES) served.set(q, await ask(q));
            });

            afterAll(async () => {
               await env?.closeAllConnections().catch(() => undefined);
               if (db) await disposeTempStorage({ db, root });
               for (const name of [warehouse, catalog]) {
                  await admin
                     ?.runSQL(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`)
                     .catch(() => undefined);
               }
               await admin?.close().catch(() => undefined);
               restore();
               await freshLoadPool();
            });

            it("builds every table, with no failure", () => {
               expect([run?.status, run?.error ?? null]).toEqual([
                  "MANIFEST_FILE_READY",
                  null,
               ]);
               const manifest = run?.manifest as { failures?: unknown };
               expect(manifest.failures ?? {}).toEqual({});
            });

            for (const q of QUERIES) {
               it(`${q.name}: served from the table, equal to live`, () => {
                  const l = live.get(q)!;
                  const s = served.get(q)!;
                  if ("error" in l) throw new Error(`live failed: ${l.error}`);
                  if ("error" in s)
                     throw new Error(`served failed: ${s.error}`);
                  expect(l.answer.servedBy ?? "live").toBe("live");
                  const widened = q.widenedOnly ?? [];
                  const without = (json: string) =>
                     JSON.stringify(
                        (JSON.parse(json) as Record<string, unknown>[]).map(
                           (row) =>
                              Object.fromEntries(
                                 Object.entries(row).filter(
                                    ([k]) => !widened.includes(k),
                                 ),
                              ),
                        ),
                     );
                  expect(without(s.answer.json)).toBe(without(l.answer.json));
                  const lRows = JSON.parse(l.answer.json) as Record<
                     string,
                     number | null
                  >[];
                  const sRows = JSON.parse(s.answer.json) as Record<
                     string,
                     number | null
                  >[];
                  for (const [i, lRow] of lRows.entries()) {
                     for (const k of widened) {
                        const lv = lRow[k];
                        const sv = sRows[i]?.[k];
                        if (lv === null || sv === null || sv === undefined) {
                           expect(sv).toBe(lv);
                           continue;
                        }
                        expect(Math.abs(sv - lv)).toBeLessThanOrEqual(
                           1e-6 * Math.max(1, Math.abs(lv)),
                        );
                        if (float4AsLive(lv) !== lv) {
                           // Live's value is no float4's text: neither may
                           // the served one be.
                           expect(float4AsLive(sv)).not.toBe(sv);
                        }
                     }
                  }
                  expect(s.answer.servedBy).toBe(q.servedBy);
                  expect(s.answer.servedFrom).toBe(tier.servedFrom);
               });
            }
         });
      }
   },
);
