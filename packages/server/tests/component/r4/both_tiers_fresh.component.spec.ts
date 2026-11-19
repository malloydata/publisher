// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A source bound in both tiers, its database table and its storage table
 * both fresh (a host placed it in a storage destination and it was also
 * built in its own warehouse): the serve verdict says what each query
 * actually reads, on every leg, under every PERSIST_STORAGE_MODE and with
 * gated sources materialized or not.
 *
 * Every leg reads the storage table first, while the mode serves it
 * (`storageLookupOf`, `servedCopyReadsTier`). Under `write-only` and `off`
 * the served copy reads the database table instead; the rollup, persisted
 * view and persisted query legs, whose binding decides their tier, answer
 * live.
 *
 * Before the fix the served copy re-rooted the source on both tables,
 * spliced its declaration twice and refused both, so its queries were
 * answered live under `on` while the verdict said `storage/serves`; under
 * `write-only` and `off` they were served from the database table while the
 * verdict said `storage/live/storage_mode`.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { __setPackageLoadPoolForTests } from "../../../src/package_load/package_load_pool";
import { unrecordedAudit } from "../../../src/service/access_record";
import { recordConnectionsAsForTests } from "../../../src/service/connection_type";
import { Environment } from "../../../src/service/environment";
import { parsePackageName } from "../../../src/service/identity";
import {
   castToDeclaredTypes,
   wideIntegerColumns,
} from "../../../src/service/materialization/stored_column_types";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import type { Package } from "../../../src/service/package";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";

const MODEL = `##! experimental { persistence givens }
given: ROLES :: string[]

source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (1, 'east', 10), (2, 'west', 20), (3, 'east', 5))
    AS t(id, region, amount)
""") extend {
  primary_key: id
}

#@ persist name="copy_h"
source: copy_h is orders -> { group_by: region; aggregate: t is amount.sum() }

#(access_filter) region in $ROLES
#@ persist name="copy_g"
source: copy_g is orders -> { group_by: region; aggregate: tg is amount.sum() }

source: rollup_h is orders extend {
  #@ preaggregate grain="region"
  measure: total is amount.sum()
}

source: view_h is orders extend {
  #@ persist
  view: by_region is { group_by: region; aggregate: vt is amount.sum(); order_by: region }
}

#@ persist
query: query_h is orders -> { group_by: region; aggregate: qt is amount.sum(), n is count(); order_by: region }
`;

type Leg = "materialized" | "rollup" | "view" | "query";

interface Case {
   label: string;
   leg: Leg;
   query: string;
   givens?: Record<string, unknown>;
   /** This case's verdict, among the package's. */
   verdictOf: (v: Verdict) => boolean;
}

interface Verdict {
   sourceName: string;
   origin: string;
   tier: string;
   verdict: string;
   reason?: string;
}

const CASES: Case[] = [
   {
      label: "the served copy",
      leg: "materialized",
      query: "run: copy_h -> { group_by: region; aggregate: x is t.sum() }",
      verdictOf: (v) => v.sourceName === "copy_h" && v.origin === "persist",
   },
   {
      label: "the served copy, through a row-level gate",
      leg: "materialized",
      query: "run: copy_g -> { group_by: region; aggregate: x is tg.sum() }",
      givens: { ROLES: ["east"] },
      verdictOf: (v) => v.sourceName === "copy_g" && v.origin === "persist",
   },
   {
      label: "the rollup leg",
      leg: "rollup",
      query: "run: rollup_h -> { group_by: region; aggregate: total }",
      verdictOf: (v) =>
         v.origin === "rollup" && v.sourceName.startsWith("rollup_h"),
   },
   {
      label: "the persisted view leg",
      leg: "view",
      query: "run: view_h -> by_region",
      verdictOf: (v) =>
         v.origin === "view" && v.sourceName.startsWith("view_h"),
   },
   {
      label: "the persisted query leg",
      leg: "query",
      query: "run: query_h",
      verdictOf: (v) =>
         v.origin === "query" && v.sourceName.startsWith("query_h"),
   },
];

interface Answer {
   rows: unknown[];
   servedBy: string;
   tier?: string;
}

/**
 * Load the package, run every case live, bind every planned table in both
 * tiers (no freshness window, so both are fresh), and run it again.
 */
async function loadAndBind(
   mode: string,
   gatedSources: boolean,
): Promise<{
   live: Map<string, Answer>;
   served: Map<string, Answer>;
   verdicts: Verdict[];
   dispose: () => Promise<void>;
}> {
   const { restore } = installSettings({
      env: {
         PERSIST_STORAGE_MODE: mode,
         PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         PUBLISHER_MATERIALIZE: "true",
         PUBLISHER_MATERIALIZE_GATED_SOURCES: String(gatedSources),
      },
   });
   await __setPackageLoadPoolForTests(null);
   // A storage build reads a warehouse; the sandbox stands in for one.
   const restoreTypes = recordConnectionsAsForTests({ duckdb: "postgres" });
   const root = await fs.mkdtemp(path.join(os.tmpdir(), "both-tiers-fresh-"));
   const dispose = async () => {
      restoreTypes();
      restore();
      await __setPackageLoadPoolForTests(null);
      await fs.rm(root, { recursive: true, force: true }).catch(() => {});
   };
   try {
      const envPath = path.join(root, "env");
      await fs.mkdir(envPath, { recursive: true });
      writePackageFiles(
         path.join(envPath, "pkg"),
         { "m.malloy": MODEL },
         { manifest: { name: "pkg", description: "both tiers fresh" } },
      );
      const env = await Environment.create("e", envPath, [] as never);
      await env.addPackage("pkg");
      const pkg: Package = await env.getPackage(parsePackageName("pkg"), false);
      const lake = new DuckDBConnection("lake", path.join(root, "lake.duckdb"));
      const config = new MalloyConfig({ connections: {} });
      config.wrapConnections(
         () => new FixedConnectionMap(new Map([["lake", lake]]), "lake"),
      );
      pkg.materialization().setServeDestinationConfig(() => config);
      const warehouse = (await pkg.getMalloyConnection(
         "duckdb",
      )) as DuckDBConnection;

      const run = async (c: Case): Promise<Answer> => {
         const r = await pkg.getModel("m.malloy")!.runQuery(
            unrecordedAudit(),
            {
               query: c.query,
               ...(c.givens ? { givens: c.givens } : {}),
            } as never,
            QueryPrivileges.of({ includeHiddenFilesAndSources: false }),
         );
         const by = r.path?.servedBy as
            | { kind?: string; tier?: string }
            | undefined;
         return {
            rows: r.compactResult as unknown[],
            servedBy: by?.kind ?? "live",
            ...(by?.tier ? { tier: by.tier } : {}),
         };
      };
      const live = new Map<string, Answer>();
      for (const c of CASES) live.set(c.label, await run(c));

      // Every planned table, built in its own warehouse and in the lake, and
      // bound in both: a database manifest entry and a storage one.
      const plan = pkg.getBuildPlan()!;
      const database: Record<string, unknown> = {};
      const stored: Record<string, unknown> = {};
      for (const entry of Object.values(plan.sources)) {
         if (stored[entry.sourceEntityId]) continue;
         const table = `t_${entry.sourceEntityId.slice(0, 12)}`;
         await warehouse.runSQL(
            `CREATE OR REPLACE TABLE ${table} AS ${entry.sql}`,
         );
         database[entry.sourceEntityId] = {
            tableName: table,
            connectionName: "duckdb",
         };
         const computed = await lake.runSQL(`DESCRIBE (${entry.sql})`);
         const select = castToDeclaredTypes(entry.sql, [], {
            exactIntegers: wideIntegerColumns(
               (
                  computed.rows as {
                     column_name: string;
                     column_type: string;
                  }[]
               ).map((r) => ({ name: r.column_name, type: r.column_type })),
            ),
         });
         await lake.runSQL(`CREATE OR REPLACE TABLE ${table} AS ${select}`);
         const described = await lake.runSQL(`DESCRIBE ${table}`);
         stored[entry.sourceEntityId] = {
            sourceEntityId: entry.sourceEntityId,
            sourceName: entry.name,
            physicalTableName: table,
            storageDestinationName: "lake",
            ...(entry.origin === "rollup" ? { origin: "rollup" } : {}),
            schema: (
               described.rows as { column_name: string; column_type: string }[]
            ).map((r) => ({ name: r.column_name, type: r.column_type })),
         };
      }
      pkg.materialization().bind({ database: database as never });
      pkg.materialization().bind({ storage: stored as never });
      for (const modelPath of pkg.getModelPaths()) {
         await pkg.getModel(modelPath)?.prepareServedCopy();
      }
      const verdicts = pkg.materialization().serveVerdicts() as Verdict[];
      const served = new Map<string, Answer>();
      for (const c of CASES) served.set(c.label, await run(c));
      return { live, served, verdicts, dispose };
   } catch (error) {
      await dispose();
      throw error;
   }
}

/** The tier each leg reads a source bound in both tiers in, under `mode`. */
function expectedTier(leg: Leg, mode: string): string | undefined {
   if (mode === "on") return "storage";
   return leg === "materialized" ? "database" : undefined;
}

for (const mode of ["on", "write-only", "off"] as const) {
   for (const gatedSources of [false, true]) {
      describe(`PERSIST_STORAGE_MODE=${mode}, gated sources ${gatedSources ? "materialized" : "not materialized"}, every table bound fresh in both tiers`, () => {
         let ctx: Awaited<ReturnType<typeof loadAndBind>> | undefined;
         beforeAll(async () => {
            ctx = await loadAndBind(mode, gatedSources);
         });
         afterAll(async () => {
            await ctx?.dispose();
         });

         for (const c of CASES) {
            const tier = expectedTier(c.leg, mode);
            it(`${c.label}: ${tier ? `served from ${tier}` : "answered live"}, as its verdict says, equal to live`, () => {
               const { live, served, verdicts } = ctx!;
               const before = live.get(c.label)!;
               const after = served.get(c.label)!;
               expect(before.servedBy).toBe("live");
               expect(after.rows).toEqual(before.rows);

               const verdict = verdicts.filter(c.verdictOf);
               expect(verdict).toHaveLength(1);
               const [v] = verdict;
               if (tier) {
                  expect(after).toMatchObject({ servedBy: c.leg, tier });
                  expect(v).toMatchObject({ tier, verdict: "serves" });
               } else {
                  expect(after.servedBy).toBe("live");
                  expect(after.tier).toBeUndefined();
                  expect(v).toMatchObject({
                     tier: "storage",
                     verdict: "live",
                     reason: "storage_mode",
                  });
               }
            });
         }
      });
   }
}
