// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A table bound on both tiers: built into the source's own database, and
 * placed by a host in a storage destination as well. Under
 * `PERSIST_STORAGE_MODE=on` the storage table answers, as it did. Under
 * `write-only` and `off` no storage table is read, so each leg answers as
 * `on`'s rules would with the storage tier removed: from its fresh database
 * table. The path report names that table as used, and a storage twin it
 * names is skipped `NOT_SERVED` with the serve verdict's `storage_mode`.
 *
 * Before the fix the rollup, persisted-view and persisted-query legs found
 * the storage binding, declined it, and answered live under `write-only` and
 * `off` (the view and query legs naming their table `STALE`), though a fresh
 * database table was bound; the served copy already read its database twin.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { unrecordedAudit } from "../../../src/service/access_record";
import {
   castToDeclaredTypes,
   wideIntegerColumns,
} from "../../../src/service/materialization/stored_column_types";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import {
   freshLoadPool,
   loadFixture,
   withSwitch,
} from "../../../src/test_helpers/conformance/package_runner";

const MODEL = `##! experimental { persistence }
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (1, 'east', 10), (2, 'west', 20), (3, 'east', 5))
    AS t(id, region, amount)
""") extend {
  primary_key: id
}

#@ persist
source: copy_d is orders -> { group_by: region; aggregate: t is amount.sum() }

source: rollup_d is orders extend {
  #@ preaggregate grain="region"
  measure: total is amount.sum()
}

source: view_d is orders extend {
  #@ persist
  view: by_region is { group_by: region; aggregate: vt is amount.sum(); order_by: region }
}

#@ persist
query: query_d is orders -> { group_by: region; aggregate: qt is amount.sum(); order_by: region }
`;

/** One query per leg, and the path report kind that answers it. */
const LEGS = [
   {
      leg: "served copy",
      kind: "materialized",
      query: "run: copy_d -> { group_by: region; aggregate: x is t.sum() }",
   },
   {
      leg: "rollup",
      kind: "rollup",
      query: "run: rollup_d -> { group_by: region; aggregate: total }",
   },
   { leg: "persisted view", kind: "view", query: "run: view_d -> by_region" },
   { leg: "persisted query", kind: "query", query: "run: query_d" },
];

interface Answer {
   rows: unknown;
   servedBy: string;
   tier?: string;
   /** The path report's skipped candidates, `REASON` or `REASON:code`. */
   skipped: string[];
}

for (const mode of ["on", "write-only", "off"] as const) {
   describe(`PERSIST_STORAGE_MODE=${mode}, a database table and a storage table bound for each leg`, () => {
      let restore: (() => void) | undefined;
      let dispose: (() => Promise<void>) | undefined;
      const live = new Map<string, Answer>();
      const served = new Map<string, Answer>();
      beforeAll(async () => {
         restore = withSwitch("on", { PERSIST_STORAGE_MODE: mode });
         await freshLoadPool();
         const fixture = await loadFixture({ "m.malloy": MODEL });
         dispose = fixture.dispose;
         const run = async (query: string): Promise<Answer> => {
            const r = await fixture.pkg
               .getModel("m.malloy")!
               .runQuery(
                  unrecordedAudit(),
                  { query } as never,
                  QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
               );
            const by = r.path?.servedBy;
            return {
               rows: r.compactResult,
               servedBy: by?.kind ?? "live",
               ...(by?.tier ? { tier: by.tier } : {}),
               skipped: (r.path?.considered ?? [])
                  .filter((c) => c.outcome === "skipped")
                  .map((c) =>
                     c.code ? `${c.reason}:${c.code}` : `${c.reason}`,
                  ),
            };
         };
         for (const l of LEGS) live.set(l.leg, await run(l.query));

         // Every table built into the source's own database and bound there.
         const bound = await fixture.bind({ includeRollups: true });
         expect(bound.buildErrors).toEqual({});
         expect(bound.built.every((b) => b.tier === "database")).toBe(true);
         // And each placed by a host in the lake as well.
         const stored: Record<string, unknown> = {};
         for (const entry of Object.values(
            fixture.pkg.getBuildPlan()!.sources,
         )) {
            const table = `h_${entry.sourceEntityId.slice(0, 12)}`;
            const computed = await fixture.lake.runSQL(
               `DESCRIBE (${entry.sql})`,
            );
            const columns = (rows: unknown) =>
               (rows as { column_name: string; column_type: string }[]).map(
                  (r) => ({ name: r.column_name, type: r.column_type }),
               );
            await fixture.lake.runSQL(
               `CREATE OR REPLACE TABLE ${table} AS ${castToDeclaredTypes(
                  entry.sql,
                  [],
                  { exactIntegers: wideIntegerColumns(columns(computed.rows)) },
               )}`,
            );
            const described = await fixture.lake.runSQL(`DESCRIBE ${table}`);
            stored[entry.sourceEntityId] = {
               sourceEntityId: entry.sourceEntityId,
               sourceName: entry.name,
               physicalTableName: table,
               storageDestinationName: "lake",
               ...(entry.origin === "rollup" ? { origin: "rollup" } : {}),
               schema: columns(described.rows),
            };
         }
         fixture.pkg.materialization().bind({ storage: stored as never });
         await fixture.pkg.getModel("m.malloy")!.prepareServedCopy();
         for (const l of LEGS) served.set(l.leg, await run(l.query));
      });
      afterAll(async () => {
         await dispose?.();
         restore?.();
         await freshLoadPool();
      });

      for (const l of LEGS) {
         // Under `on` the served copy does not re-root `copy_d` with both
         // tables bound (a `served-copy-not-served` warning), so it answers
         // live there, as it did before this fix: pinned, not endorsed.
         const onLive = mode === "on" && l.kind === "materialized";
         const tier = mode === "on" ? "storage" : "database";
         it(`the ${l.leg} leg: ${onLive ? "answered live" : `answered from its ${tier} table`}, equal to live`, () => {
            const before = live.get(l.leg)!;
            const after = served.get(l.leg)!;
            expect(before.servedBy).toBe("live");
            expect(after.rows).toEqual(before.rows);
            if (onLive) {
               expect(after.servedBy).toBe("live");
               expect(after.skipped).toEqual(["NOT_SERVED:served_copy"]);
               return;
            }
            expect(after.servedBy).toBe(l.kind);
            expect(after.tier).toBe(tier);
            if (mode === "on") {
               expect(after.skipped).toEqual([]);
            } else {
               // At most the storage twin, as the storage mode leaves it.
               expect(
                  after.skipped.filter((s) => s !== "NOT_SERVED:storage_mode"),
               ).toEqual([]);
            }
         });
      }
   });
}
