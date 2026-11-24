// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `storage=` variants of the chained shapes the database specs prove
 * (extension_chain_pair_parent, extension_chain_left_out_terms,
 * pair_table_view_over_stored), and the chained shapes refused at plan,
 * each held to one of two outcomes:
 *
 *  - built over its upstream's table, never by recomputing the upstream from
 *    raw (the drop-raw proof: the upstream is built alone, every raw table
 *    is then emptied out of band, and the downstream, built next, still
 *    builds with `upstreamReuse: "reused"`, names the upstream's table in
 *    `builtFrom`, and serves what live answered before the raw rows went);
 *  - refused at plan as `upstream_table_unusable`, with the reason named, no
 *    table and no failed build, and its query answered equal to live.
 *
 * Opt-in: set PUBLISHER_TEST_POSTGRES_URL, for the warehouse (a DuckDB
 * warehouse is not read through to a `storage=` build) and the DuckLake
 * catalog; the build in the destination runs on DuckDB.
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
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import type { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { openTempStorage } from "../../src/test_helpers/temp_storage";

const ENV = "e1";
const PKG = "pkg";
const POSTGRES_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;

/**
 * The raw tables, in the warehouse's `public` schema. Order 3 has no item
 * (a childless parent). Two event rows are older than the 30-day window and
 * two far ahead of it.
 */
const RAW: Record<string, string> = {
   sv_orders: `SELECT * FROM (VALUES
    (1, 'x', CAST(10.0 AS DECIMAL(10,1))),
    (2, 'x', CAST(20.0 AS DECIMAL(10,1))),
    (3, 'y', CAST(5.0 AS DECIMAL(10,1))),
    (4, 'z', CAST(7.0 AS DECIMAL(10,1)))
  ) AS t(id, cust, amount)`,
   sv_items: `SELECT * FROM (VALUES
    (100, 1, CAST(3.00 AS DECIMAL(10,2))),
    (101, 1, CAST(4.50 AS DECIMAL(10,2))),
    (102, 2, CAST(6.00 AS DECIMAL(10,2))),
    (103, 4, CAST(1.25 AS DECIMAL(10,2)))
  ) AS t(item_id, order_id, price)`,
   sv_events: `SELECT * FROM (VALUES
    (1, 'east', CAST(10.5 AS DECIMAL(10,1)), TIMESTAMP '2020-01-01 00:00:00'),
    (2, 'west', CAST(20.0 AS DECIMAL(10,1)), TIMESTAMP '2020-02-01 00:00:00'),
    (3, 'east', CAST(5.5 AS DECIMAL(10,1)), TIMESTAMP '2100-01-01 00:00:00'),
    (4, 'west', CAST(3.0 AS DECIMAL(10,1)), TIMESTAMP '2100-02-01 00:00:00'),
    (5, 'east', CAST(8.0 AS DECIMAL(10,1)), TIMESTAMP '2020-03-01 00:00:00')
  ) AS t(id, region, amount, ts)`,
};

const HEAD = `##! experimental { persistence }
`;

/** A pair-table parent (an inlined join_many), stored where `placement` says. */
const pairParent = (
   placement: string,
) => `${HEAD}source: items is warehouse.table('public.sv_items') extend {
  primary_key: item_id
}

#(materialize)${placement} inline=["items"]
source: orders is warehouse.table('public.sv_orders') extend {
  primary_key: id
  join_many: items is items on items.order_id = id
  measure: total is amount.sum()
  measure: price_sum is items.price.sum()
}
`;

/** A parent whose `where:` holds a current-time upper bound, left out of its table. */
const windowParent = (
   placement: string,
) => `${HEAD}source: raw is warehouse.table('public.sv_events')

#(materialize)${placement}
source: older is raw extend {
  where: ts < now - 30 days
}
`;

/**
 * No string is grouped, filtered or ordered, so every answer compares as
 * live's: a Postgres warehouse compares strings under its database's
 * collation, a destination byte-exact, and a query that orders a string
 * answers live (row s-later-window), which would prove nothing here.
 */

/** The pair extension's rows, per order, as text: one row per parent. */
const PAIR_EXT_QUERY = `run: ext -> {
  group_by: id
  aggregate: n is count(), amount_sum is total, big_n is count() { where: big }
} -> {
  select: id, n_text is n::string, amount_text is amount_sum::string, big_text is big_n::string
  order_by: id
}`;

/** The window extension's rows, as text: the window re-applied at read. */
const WINDOW_EXT_QUERY = `run: ext -> {
  group_by: id
  aggregate: n is count(), amount_sum is amount.sum(), big_n is count() { where: big }
} -> {
  select: id, n_text is n::string, amount_text is amount_sum::string, big_text is big_n::string
  order_by: id
}`;

/**
 * A view or query's rows over the pair table: symmetric aggregates, compared
 * as numbers, not text. A destination stores a Postgres `numeric` expression
 * as `DECIMAL(38,s)` with trailing zeros dropped (docs/materialization.md),
 * so its `::string` reads `3.75` where Postgres's reads `3.7500000000000000`:
 * the same value.
 */
const PAIR_AGGREGATES = `group_by: id
    aggregate: total, n is count(), price_sum, avg_price is items.price.avg()`;
const PAIR_AS_TEXT = `-> {
  select: id, total, n, price_sum, avg_price
  order_by: id
}`;

/** Each value a number reads as, so `"3.75"` and `3.75` compare equal. */
const asNumbers = (rows: unknown): unknown =>
   (rows as Record<string, unknown>[]).map((r) =>
      Object.fromEntries(
         Object.entries(r).map(([k, v]) => [
            k,
            v === null || v === undefined || Number.isNaN(Number(v))
               ? v
               : Number(v),
         ]),
      ),
   );

interface Mechanism {
   label: string;
   /** The v18 row and cell it covers. */
   cell: string;
   files: Record<string, string>;
   /** Built alone first; for a refusal, the source the refused one reads. */
   upstream: string;
   /** The downstream's entry `sourceName`, or its refusal's `name`. */
   downstream: RegExp;
   query: string;
   /**
    * Refused at plan as `upstream_table_unusable`, its message containing
    * this; built over the upstream's table when absent.
    */
   refused?: string;
}

const MECHANISMS: Mechanism[] = [
   {
      label: "a storage= extension of a database pair-table parent",
      cell: "b-ext-pair ms",
      files: {
         "m.malloy": `${pairParent("")}
#(materialize) storage="lake"
source: ext is orders extend { dimension: big is amount > 6 }
`,
      },
      upstream: "orders",
      downstream: /^ext$/,
      query: PAIR_EXT_QUERY,
   },
   {
      label: "a storage= extension of a pair-table parent in its destination",
      cell: "b-ext-pair ms",
      files: {
         "m.malloy": `${pairParent(' storage="lake"')}
#(materialize) storage="lake"
source: ext is orders extend { dimension: big is amount > 6 }
`,
      },
      upstream: "orders",
      downstream: /^ext$/,
      query: PAIR_EXT_QUERY,
   },
   {
      label: "a storage= extension of a database parent's upper-bound window",
      cell: "b-ext-window-parent ms",
      files: {
         "m.malloy": `${windowParent("")}
#(materialize) storage="lake"
source: ext is older extend { dimension: big is amount > 6 }
`,
      },
      upstream: "older",
      downstream: /^ext$/,
      query: WINDOW_EXT_QUERY,
   },
   {
      label: "a storage= extension of an upper-bound window parent in its destination",
      cell: "b-ext-window-parent ms",
      files: {
         "m.malloy": `${windowParent(' storage="lake"')}
#(materialize) storage="lake"
source: ext is older extend { dimension: big is amount > 6 }
`,
      },
      upstream: "older",
      downstream: /^ext$/,
      query: WINDOW_EXT_QUERY,
   },
   {
      label: "a storage= persisted view over a database pair table",
      cell: "v-over-pair vs",
      files: {
         "m.malloy": `${pairParent("")}
source: s is orders extend {
  #@ persist storage="lake"
  view: by_order is { ${PAIR_AGGREGATES} }
}
`,
      },
      upstream: "orders",
      downstream: /^s__by_order__view$/,
      query: `run: s -> by_order ${PAIR_AS_TEXT}`,
   },
   {
      label: "a storage= persisted query over a database pair table",
      cell: "v-over-pair qs",
      files: {
         "m.malloy": `${pairParent("")}
#@ persist storage="lake"
query: q is orders -> { ${PAIR_AGGREGATES} }
`,
      },
      upstream: "orders",
      downstream: /^q__query$/,
      query: `run: q ${PAIR_AS_TEXT}`,
   },
   {
      label: "a storage= persisted view over a pair table in its destination",
      cell: "v-over-pair vs",
      files: {
         "m.malloy": `${pairParent(' storage="lake"')}
source: s is orders extend {
  #@ persist storage="lake"
  view: by_order is { ${PAIR_AGGREGATES} }
}
`,
      },
      upstream: "orders",
      downstream: /^s__by_order__view$/,
      query: `run: s -> by_order ${PAIR_AS_TEXT}`,
      refused:
         "its #(materialize) upstream 'orders' has no table it can build from: its table is in a storage= destination",
   },
   {
      label: "a storage= persisted query over a pair table in its destination",
      cell: "v-over-pair qs",
      files: {
         "m.malloy": `${pairParent(' storage="lake"')}
#@ persist storage="lake"
query: q is orders -> { ${PAIR_AGGREGATES} }
`,
      },
      upstream: "orders",
      downstream: /^q__query$/,
      query: `run: q ${PAIR_AS_TEXT}`,
      refused:
         "its #(materialize) upstream 'orders' has no table it can build from: its table is in a storage= destination",
   },
   {
      label: "a database #@ persist over a storage= #@ persist",
      cell: "b-strict-plan pc",
      files: {
         "m.malloy": `${HEAD}source: raw is warehouse.table('public.sv_orders')

#@ persist storage="lake"
source: a is raw -> { select: cust, amount }

#@ persist name="public.sv_b"
source: b is a -> { group_by: cust; aggregate: total is amount.sum() }
`,
      },
      upstream: "a",
      downstream: /^b$/,
      query: "run: b -> { aggregate: n is count(), total_text is total.sum()::string }",
      refused:
         "its upstream 'a' is stored in the storage= destination 'lake', whose table a build in the warehouse cannot read",
   },
   {
      label: "a database rollup over an extension of a #(materialize) source in another file",
      cell: "b-ext-crossfile rc",
      files: {
         "a.malloy": `${HEAD}source: raw is warehouse.table('public.sv_orders')
#(materialize)
source: a is raw extend { dimension: cust2 is cust }
`,
         "m.malloy": `${HEAD}import "a.malloy"
source: rolled is a extend {
  #(rollup) grain=["cust"]
  measure: total is amount.sum()
}
`,
      },
      upstream: "a",
      downstream: /^rolled__rollup__cust__/,
      query: "run: rolled -> { aggregate: total } -> { select: total_text is total::string }",
      refused:
         "its #(materialize) upstream 'rolled' has no table it can build from",
   },
   {
      label: "a storage= rollup over an extension of a #(materialize) source in another file",
      cell: "b-ext-crossfile rs",
      files: {
         "a.malloy": `${HEAD}source: raw is warehouse.table('public.sv_orders')
#(materialize) storage="lake"
source: a is raw extend { dimension: cust2 is cust }
`,
         "m.malloy": `${HEAD}import "a.malloy"
source: rolled is a extend {
  #(rollup) grain=["cust"] storage="lake"
  measure: total is amount.sum()
}
`,
      },
      upstream: "a",
      downstream: /^rolled__rollup__cust__/,
      query: "run: rolled -> { aggregate: total } -> { select: total_text is total::string }",
      refused:
         "its #(materialize) upstream 'rolled' has no table it can build from",
   },
];

interface Entry {
   sourceName?: string;
   sourceEntityId: string;
   physicalTableName: string;
   storageDestinationName?: string;
   upstreamReuse?: string;
   builtAt?: string;
   builtFrom?: {
      sourceEntityId: string;
      physicalTableName: string;
      builtAt?: string;
   }[];
}

const entriesOf = (m: Materialization): Entry[] =>
   Object.values(
      (m.manifest as { entries?: Record<string, Entry> } | null)?.entries ?? {},
   );

const failuresOf = (m: Materialization): { sourceName?: string }[] =>
   Object.values(
      (
         m.manifest as {
            failures?: Record<string, { sourceName?: string }>;
         } | null
      )?.failures ?? {},
   );

/** A Postgres warehouse and a DuckLake destination catalogued on the same server. */
async function connections(root: string) {
   const token = `${process.pid}_${Date.now()}`;
   const dbs = [`sv_wh_${token}`, `sv_lake_${token}`];
   const admin = new PostgresConnection({
      name: "admin",
      connectionString: POSTGRES_URL!,
   });
   for (const db of dbs) await admin.runSQL(`CREATE DATABASE ${db}`);
   const parsed = new URL(POSTGRES_URL!);
   const pg = {
      host: parsed.hostname,
      port: Number(parsed.port || 5432),
      userName: decodeURIComponent(parsed.username),
      password: decodeURIComponent(parsed.password),
   };
   const bucket = path.join(root, "bucket_lake");
   fs.mkdirSync(bucket);
   return {
      warehouse: {
         name: "warehouse",
         type: "postgres",
         postgresConnection: { ...pg, databaseName: dbs[0] },
      },
      destinations: [
         {
            name: "lake",
            type: "ducklake",
            ducklakeConnection: {
               catalog: { postgresConnection: { ...pg, databaseName: dbs[1] } },
               storage: { bucketUrl: bucket },
            },
         },
      ],
      drop: async () => {
         for (const db of dbs) {
            await admin
               .runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
               .catch(() => undefined);
         }
         await admin.close().catch(() => undefined);
      },
   };
}

for (const mechanism of POSTGRES_URL ? MECHANISMS : []) {
   describe(`${mechanism.label} (${mechanism.cell}; Postgres, DuckLake)`, () => {
      let restore: () => void;
      let root: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment;
      let service: MaterializationService;
      let drop: () => Promise<void> = async () => {};
      let first: Materialization;
      let second: Materialization;
      let liveBefore: unknown;
      let served: { rows: unknown; kind?: string };

      const ask = async (query: string) => {
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const result = await pkg
            .getModel("m.malloy")!
            .runQuery(
               unrecordedAudit(),
               { query } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         return {
            rows: JSON.parse(JSON.stringify(result.compactResult)) as unknown,
            kind: result.path?.servedBy?.kind,
         };
      };
      const build = async (opts: object): Promise<Materialization> => {
         const created = await service.createMaterialization(ENV, PKG, opts);
         const running = (
            service as unknown as {
               runningAbortControllers: Map<string, unknown>;
            }
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
      };
      const refusalOf = async () => {
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         return Object.values(pkg.getBuildPlan()?.refusedSources ?? {}).find(
            (r) => mechanism.downstream.test(r.name),
         );
      };

      beforeAll(async () => {
         restore = installSettings({
            env: {
               PUBLISHER_MATERIALIZE: "true",
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         let envPath: string;
         ({ root, envPath, db, repo } = await openTempStorage("sv-", {
            id: "env-1",
            name: ENV,
         }));
         writePackageFiles(path.join(envPath, PKG), mechanism.files, {
            manifest: { name: PKG, description: "fixture" },
         });
         const made = await connections(root);
         drop = made.drop;
         env = await Environment.create(
            ENV,
            envPath,
            [made.warehouse as never],
            made.destinations as never[],
         );
         const warehouse = await env.getMalloyConnection("warehouse");
         for (const [name, sql] of Object.entries(RAW)) {
            await warehouse.runSQL(`CREATE TABLE public.${name} AS ${sql}`);
         }
         await env.addPackage(PKG);
         service = serviceOverEnvironment(
            () => repo,
            () => env,
         );
         liveBefore = (await ask(mechanism.query)).rows;
         first = await build({ sourceNames: [mechanism.upstream] });
         // Out of band: the raw rows are gone, unless the downstream is
         // refused (then its query answers live, from them). A downstream
         // that recomputed its upstream from raw would hold no rows.
         if (!mechanism.refused) {
            for (const name of Object.keys(RAW)) {
               await warehouse.runSQL(`DELETE FROM public.${name}`);
            }
         }
         second = await build({});
         served = await ask(mechanism.query);
      }, 300_000);
      afterAll(async () => {
         await env?.closeAllConnections().catch(() => undefined);
         await db?.close();
         await drop();
         if (root) fs.rmSync(root, { recursive: true, force: true });
         restore?.();
      });

      it("builds the upstream alone first", () => {
         expect([first.status, first.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         expect(
            entriesOf(first).filter((e) => e.sourceName === mechanism.upstream),
         ).toHaveLength(1);
      });

      if (mechanism.refused) {
         it("refuses the downstream at plan as upstream_table_unusable, naming why, and never builds it", async () => {
            const refusal = await refusalOf();
            expect(refusal?.reason).toBe("upstream_table_unusable");
            expect(refusal?.message).toContain(mechanism.refused);
            expect(refusal?.message).toMatch(
               /a downstream is never built by recomputing its upstreams? from raw/,
            );
            expect(
               entriesOf(second).filter((e) =>
                  mechanism.downstream.test(e.sourceName ?? ""),
               ),
            ).toEqual([]);
            expect(
               failuresOf(second).filter((f) =>
                  mechanism.downstream.test(f.sourceName ?? ""),
               ),
            ).toEqual([]);
         });

         it("answers its query equal to live", () => {
            expect(asNumbers(served.rows)).toEqual(asNumbers(liveBefore));
            expect((liveBefore as unknown[]).length).toBeGreaterThan(0);
         });
      } else {
         it("is planned: no refusal names it", async () => {
            expect(await refusalOf()).toBeUndefined();
         });

         it("builds the downstream into its destination, reusing the upstream's table, named in builtFrom", () => {
            expect([second.status, second.error]).toEqual([
               "MANIFEST_FILE_READY",
               null,
            ]);
            const upstream = entriesOf(first).find(
               (e) => e.sourceName === mechanism.upstream,
            )!;
            const downstream = entriesOf(second).filter((e) =>
               mechanism.downstream.test(e.sourceName ?? ""),
            );
            expect(downstream).toHaveLength(1);
            expect(downstream[0].storageDestinationName).toBe("lake");
            expect(downstream[0].upstreamReuse).toBe("reused");
            expect(
               (downstream[0].builtFrom ?? []).map((b) => [
                  b.sourceEntityId,
                  b.physicalTableName,
                  b.builtAt,
               ]),
            ).toEqual([
               [
                  upstream.sourceEntityId,
                  upstream.physicalTableName,
                  upstream.builtAt,
               ],
            ]);
         });

         it("serves its table, equal to live before the raw rows went", () => {
            expect(served.kind).not.toBe("live");
            expect(asNumbers(served.rows)).toEqual(asNumbers(liveBefore));
            expect((liveBefore as unknown[]).length).toBeGreaterThan(0);
         });
      }
   });
}
