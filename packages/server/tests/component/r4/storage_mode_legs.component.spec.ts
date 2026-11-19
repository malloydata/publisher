// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every leg that can read a storage table reads one only while
 * `PERSIST_STORAGE_MODE=on`. Under `write-only` a build writes the
 * destination but serving stays live (docs/configuration.md), and under
 * `off` the destination is inert, so with storage tables bound the served
 * copy, the rollup leg, the persisted-view leg and the persisted-query leg
 * all answer live, and the path report says so. Before the fix the rollup,
 * persisted-view and persisted-query legs read the destination under
 * `write-only`, while the serve verdict said `live/storage_mode`.
 *
 * Each leg is bound twice: to a table its own `storage=` names, and to a
 * storage table a host placed for a source that names no destination (a
 * storage manifest entry for it). The gated rollups are the control: the
 * one whose gate column is in its grain serves from storage under `on`, as
 * it did, and the one whose gate is not is never read.
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

const ORDERS = `duckdb.sql("""
  SELECT * FROM (VALUES (1, 'east', 10, 0.5::DOUBLE), (2, 'west', 20, 0.25::DOUBLE), (3, 'east', 5, 0.5::DOUBLE))
    AS t(id, region, amount, w)
""") extend {
  primary_key: id
}`;

/** One leg per pair: its `storage=` form, then its host-placed form. */
const MODEL = `##! experimental { persistence }
source: orders is ${ORDERS}

#@ persist storage="lake"
source: copy_s is orders -> { group_by: region; aggregate: t is amount.sum() }

#@ persist name="copy_h"
source: copy_h is orders -> { group_by: region; aggregate: t is amount.sum() }

source: rollup_s is orders extend {
  #@ preaggregate grain="region" storage="lake"
  measure: total is amount.sum()
}

source: rollup_h is orders extend {
  #@ preaggregate grain="region"
  measure: total is amount.sum()
}

source: view_s is orders extend {
  #@ persist storage="lake"
  view: by_region is { group_by: region; aggregate: vt is amount.sum() }
}

source: view_h is orders extend {
  #@ persist
  view: by_region is { group_by: region; aggregate: vt is amount.sum(); order_by: region }
}

#@ persist storage="lake"
query: query_s is orders -> { group_by: region; aggregate: qt is amount.sum(); order_by: region }

#@ persist
query: query_h is orders -> { group_by: region; aggregate: qt is amount.sum(), n is count(); order_by: region }
`;

const GATED = `##! experimental { persistence givens }
given: ROLES :: string[]
given: W :: number

source: orders is ${ORDERS}

#(access_filter) region in $ROLES
source: gated_in_grain is orders extend {
  #@ preaggregate grain="region"
  measure: total is amount.sum()
}

#(access_filter) w = $W
source: gated_off_grain is orders extend {
  #@ preaggregate grain="region" storage="lake"
  measure: total is amount.sum()
}
`;

interface Case {
   label: string;
   model: string;
   query: string;
   /** The leg that answers under `on`: a path report `servedBy.kind`, or `live`. */
   onServedBy: string;
   givens?: Record<string, unknown>;
}

const LEGS: Case[] = [
   [
      "served copy",
      "copy",
      "materialized",
      "run: X -> { group_by: region; aggregate: x is t.sum() }",
   ],
   [
      "rollup",
      "rollup",
      "rollup",
      "run: X -> { group_by: region; aggregate: total }",
   ],
   ["persisted view", "view", "view", "run: X -> by_region"],
   ["persisted query", "query", "query", "run: X"],
].flatMap(([leg, stem, kind, query]) =>
   (
      [
         ["storage=", "s"],
         ["host-placed", "h"],
      ] as const
   ).map(([placed, suffix]) => ({
      label: `the ${leg} leg, ${placed}`,
      model: "m.malloy",
      query: query.replace("X", `${stem}_${suffix}`),
      onServedBy: kind,
   })),
);

const GATED_CASES: Case[] = [
   {
      label: "a gated rollup whose gate column is in its grain",
      model: "gated.malloy",
      query: "run: gated_in_grain -> { group_by: region; aggregate: total }",
      givens: { ROLES: ["east"] },
      onServedBy: "rollup",
   },
   {
      label: "a gated storage= rollup whose gate column is not",
      model: "gated.malloy",
      query: "run: gated_off_grain -> { group_by: region; aggregate: total }",
      givens: { W: 0.5 },
      onServedBy: "live",
   },
];

interface Answer {
   rows: unknown[];
   servedFrom: string | null;
   servedBy: string;
   tier?: string;
}

/**
 * Load the package under `mode`, run every case live, then bind every
 * planned table into the `lake` destination and run it again.
 */
async function loadAndBind(mode: string): Promise<{
   live: Map<string, Answer>;
   served: Map<string, Answer>;
   dispose: () => Promise<void>;
}> {
   const { restore } = installSettings({
      env: {
         PERSIST_STORAGE_MODE: mode,
         PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
         PUBLISHER_MATERIALIZE: "true",
      },
   });
   await __setPackageLoadPoolForTests(null);
   // A storage build reads a warehouse; the sandbox stands in for one.
   const restoreTypes = recordConnectionsAsForTests({ duckdb: "postgres" });
   const root = await fs.mkdtemp(path.join(os.tmpdir(), "storage-mode-legs-"));
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
         { "m.malloy": MODEL, "gated.malloy": GATED },
         { manifest: { name: "pkg", description: "storage mode legs" } },
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

      const run = async (c: Case): Promise<Answer> => {
         const r = await pkg
            .getModel(c.model)!
            .runQuery(
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
            servedFrom: r.servedFrom ?? null,
            servedBy: by?.kind ?? "live",
            ...(by?.tier ? { tier: by.tier } : {}),
         };
      };
      const cases = [...LEGS, ...GATED_CASES];
      const live = new Map<string, Answer>();
      for (const c of cases) live.set(c.label, await run(c));

      // Every planned table, built into the lake and bound as a storage
      // manifest entry: a `storage=` source's under its own destination, and
      // every other one as a host would place it there.
      const plan = pkg.getBuildPlan()!;
      const stored: Record<string, unknown> = {};
      for (const entry of Object.values(plan.sources)) {
         if (stored[entry.sourceEntityId]) continue;
         const table = `t_${entry.sourceEntityId.slice(0, 12)}`;
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
            storageDestinationName: entry.annotationFields?.storage ?? "lake",
            ...(entry.origin === "rollup" ? { origin: "rollup" } : {}),
            schema: (
               described.rows as { column_name: string; column_type: string }[]
            ).map((r) => ({ name: r.column_name, type: r.column_type })),
         };
      }
      pkg.materialization().bind({ storage: stored as never });
      for (const modelPath of pkg.getModelPaths()) {
         await pkg.getModel(modelPath)?.prepareServedCopy();
      }
      const served = new Map<string, Answer>();
      for (const c of cases) served.set(c.label, await run(c));
      return { live, served, dispose };
   } catch (error) {
      await dispose();
      throw error;
   }
}

for (const mode of ["on", "write-only", "off"] as const) {
   describe(`PERSIST_STORAGE_MODE=${mode}, every table bound in storage`, () => {
      let live: Map<string, Answer>;
      let served: Map<string, Answer>;
      let dispose: (() => Promise<void>) | undefined;
      beforeAll(async () => {
         ({ live, served, dispose } = await loadAndBind(mode));
      });
      afterAll(async () => {
         await dispose?.();
      });

      for (const c of [...LEGS, ...GATED_CASES]) {
         const fromStorage = mode === "on" && c.onServedBy !== "live";
         it(`${c.label}: ${fromStorage ? `served from storage by the ${c.onServedBy} leg` : "answered live"}, equal to live`, () => {
            const before = live.get(c.label)!;
            const after = served.get(c.label)!;
            expect(before.servedBy).toBe("live");
            expect(after.rows).toEqual(before.rows);
            if (fromStorage) {
               expect(after.servedFrom).toBe("storage");
               expect(after.servedBy).toBe(c.onServedBy);
               expect(after.tier).toBe("storage");
            } else {
               // Never the destination: no storage answer, no storage tier.
               expect(after.servedFrom).not.toBe("storage");
               expect(after.tier).not.toBe("storage");
               expect(after.servedBy).toBe("live");
            }
         });
      }
   });
}
