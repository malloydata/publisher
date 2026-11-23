// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as sinon from "sinon";
import { logger } from "../../../logger";
import { resetMaterializationTelemetryForTesting } from "../../../materialization_metrics";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../test_helpers/metrics_harness";
import type { IncrementalDeclaration } from "../domain/incremental";
import {
   MERGE_KEY_GUARD_MESSAGE,
   PAIR_GUARD_MESSAGE,
   warehouseDeltaTarget,
   type DeltaTarget,
   type SqlRunner,
} from "./incremental_apply";
import {
   advanceLedgerAfterSeed,
   applyIncrementalStep,
   planSourceRefresh,
   type IncrementalRunContext,
} from "./incremental_build";
import { StorageBuildStrategy } from "./storage_build_strategy";

/**
 * The incremental reason codes a refresh falls back on when something it
 * reads or writes fails: an unreadable ledger, a delta that cannot be planned,
 * an unreadable frontier, a pair delta whose parent-row check fails, and a
 * stored source chained on a stored parent. Each one rebuilds (or skips)
 * rather than failing the run, and is counted under its own reason.
 */

const NOW = new Date("2024-07-01T00:00:00Z");
const COLUMNS = ["order_date", "region", "revenue"];

const LINEAGE = {
   physicalTableName: "analytics.daily",
   connectionName: "wh",
   sourceEntityId: "sid",
   watermarkName: "order_date",
   watermarkType: "date",
   mergeKeys: [] as string[],
   scopeColumns: [] as string[],
   strategy: "range_replace" as const,
};

const LEDGER = {
   environmentId: "env",
   packageName: "pkg",
   sourceEntityId: "sid",
   coveredThroughValue: "2024-06-01",
   coveredThroughType: "date",
   watermarkDimension: "order_date",
   mergeKeyDimensions: [] as string[],
   derivedStrategy: "range_replace" as const,
   physicalTableName: "analytics.daily",
   connectionName: "wh",
   advancedByMaterializationId: "run-1",
   advancedAt: new Date("2024-06-01T00:00:00Z"),
   createdAt: new Date("2024-05-01T00:00:00Z"),
};

/**
 * Answers the planner's probes: the table's columns, its emptiness, its newest
 * stored watermark, the frontier.
 */
function runner(
   answers: { frontierFails?: boolean; columns?: string[] } = {},
): SqlRunner {
   return async (sql) => {
      if (/information_schema/i.test(sql)) {
         return {
            rows: (answers.columns ?? COLUMNS).map((c) => ({
               column_name: c,
            })),
         };
      }
      if (sql.includes("LIMIT 1")) return { rows: [{ present: 1 }] };
      // The newest stored watermark a date delta starts from.
      if (/ <= (DATE|TIMESTAMP) '/.test(sql)) {
         return { rows: [{ watermark_max: "2024-05-31" }] };
      }
      if (sql.includes("watermark_max")) {
         if (answers.frontierFails) throw new Error("permission denied");
         return { rows: [{ watermark_max: 99 }] };
      }
      throw new Error(`unexpected probe: ${sql}`);
   };
}

function target(
   lineage: typeof LINEAGE = LINEAGE,
   answers?: Parameters<typeof runner>[0],
): DeltaTarget {
   return warehouseDeltaTarget({
      dialect: "postgres",
      runner: runner(answers),
      quotedTablePath: `"analytics"."daily"`,
      lineage,
      sourceSQL: "SELECT * FROM base",
   });
}

function context(
   ledger: Partial<IncrementalRunContext["ledger"]> = {},
): IncrementalRunContext {
   return {
      environmentId: "env",
      packageName: "pkg",
      materializationId: "run",
      forceRefresh: false,
      now: NOW,
      declarations: {},
      ledger: {
         getIncrementalLedgerEntry: async () => LEDGER,
         deleteIncrementalLedgerEntry: async () => undefined,
         ...ledger,
      },
   } as unknown as IncrementalRunContext;
}

describe("incremental refresh fallback reasons", () => {
   let harness: MetricsHarness;
   let warn: sinon.SinonStub;
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetMaterializationTelemetryForTesting();
      warn = sinon.stub(logger, "warn");
   });
   afterEach(async () => {
      warn.restore();
      resetMaterializationTelemetryForTesting();
      await harness.shutdown();
   });

   const counted = (step: string, reason: string) =>
      harness.collectCounter(
         "publisher_materialization_incremental_step_total",
         { step, reason },
      );

   it("seeds with ledger_unreadable when the ledger cannot be read", async () => {
      const step = await planSourceRefresh({
         context: context({
            getIncrementalLedgerEntry: async () => {
               throw new Error("connection reset");
            },
         }),
         lineage: LINEAGE,
         target: target(),
         columns: COLUMNS,
      });
      expect(step).toMatchObject({
         mode: "seed",
         reasonCode: "ledger_unreadable",
      });
      expect(step.mode === "seed" && step.reason).toContain("connection reset");
   });

   it("seeds with ledger_entry_malformed when the caller's entry for the table was dropped", async () => {
      const caller = (forceRefresh = false) =>
         ({
            ...context(),
            forceRefresh,
            callerLedger: new Map(),
            malformedCallerLedger: new Map([
               [
                  "analytics.daily",
                  "'strategy' must be 'merge' or 'range_replace'",
               ],
            ]),
         }) as unknown as IncrementalRunContext;
      const step = await planSourceRefresh({
         context: caller(),
         lineage: LINEAGE,
         target: target(),
         columns: COLUMNS,
      });
      expect(step).toMatchObject({
         mode: "seed",
         reasonCode: "ledger_entry_malformed",
      });
      expect(step.mode === "seed" && step.reason).toContain(
         "'strategy' must be",
      );

      // Counted and logged under its own reason, not as no_boundary.
      await applyIncrementalStep({
         context: caller(),
         lineage: LINEAGE,
         step,
         target: target(),
         sourceName: "daily",
      });
      expect(await counted("seed", "ledger_entry_malformed")).toBe(1);
      expect(await counted("seed", "no_boundary")).toBe(0);
      expect(
         warn
            .getCalls()
            .some(
               (c) =>
                  c.args[0] === "Rebuilding an incremental source in full" &&
                  c.args[1]?.reasonCode === "ledger_entry_malformed",
            ),
      ).toBe(true);

      // A reseed ignores the entry, dropped or not.
      expect(
         await planSourceRefresh({
            context: caller(true),
            lineage: LINEAGE,
            target: target(),
            columns: COLUMNS,
         }),
      ).toMatchObject({ mode: "seed", reasonCode: "forced" });
      // Another table with no entry is the ordinary first build.
      expect(
         await planSourceRefresh({
            context: caller(),
            lineage: { ...LINEAGE, physicalTableName: "analytics.other" },
            target: target(),
            columns: COLUMNS,
         }),
      ).toMatchObject({ mode: "seed", reasonCode: "no_boundary" });
   });

   it("seeds with plan_error when the delta cannot be planned", async () => {
      const failing: DeltaTarget = {
         ...target(),
         deltaRows: async () => {
            throw new Error("cannot render the range");
         },
      };
      const step = await planSourceRefresh({
         context: context(),
         lineage: LINEAGE,
         target: failing,
         columns: COLUMNS,
      });
      expect(step).toMatchObject({ mode: "seed", reasonCode: "plan_error" });
      expect(step.mode === "seed" && step.reason).toContain(
         "cannot render the range",
      );
   });

   it("skips with frontier_unreadable when a number watermark's frontier cannot be read", async () => {
      const numeric = {
         ...LINEAGE,
         watermarkName: "seq",
         watermarkType: "number",
      };
      const step = await planSourceRefresh({
         context: context({
            getIncrementalLedgerEntry: async () => ({
               ...LEDGER,
               coveredThroughValue: "10",
               coveredThroughType: "number",
               watermarkDimension: "seq",
            }),
         }),
         lineage: numeric,
         target: target(numeric, {
            frontierFails: true,
            columns: ["seq", "region", "revenue"],
         }),
         columns: ["seq", "region", "revenue"],
      });
      expect(step).toMatchObject({
         mode: "skip",
         reasonCode: "frontier_unreadable",
         coveredThrough: { malloyType: "number", value: "10" },
      });

      // Applied, the skip is reported under its reason and keeps the boundary.
      const applied = await applyIncrementalStep({
         context: context(),
         lineage: numeric,
         step,
         target: target(numeric),
         sourceName: "daily",
      });
      expect(applied).toMatchObject({ applied: true, refresh: "none" });
      expect(await counted("skip", "frontier_unreadable")).toBe(1);
   });

   it("rebuilds with pair_parent_row when a pair delta's parent-row check fails", async () => {
      const pair = {
         ...LINEAGE,
         pair: { parentKey: "id", parentRow: "__parent_row" },
      } as unknown as typeof LINEAGE;
      const failing: DeltaTarget = {
         ...target(pair),
         runner: async () => {
            throw new Error(`Invalid Input Error: ${PAIR_GUARD_MESSAGE}`);
         },
      };
      const applied = await applyIncrementalStep({
         context: context(),
         lineage: pair,
         step: {
            mode: "delta",
            start: { malloyType: "date", value: "2024-06-01" },
            end: { malloyType: "date", value: "2024-07-01" },
            coveredThrough: { malloyType: "date", value: "2024-07-01" },
            statements: ["DELETE FROM t", "INSERT INTO t SELECT 1"],
         } as never,
         target: failing,
         sourceName: "orders_all",
      });
      expect(applied).toEqual({ applied: false });
      expect(await counted("seed", "pair_parent_row")).toBe(1);
   });

   it("rebuilds with merge_key_not_unique when a merge's delta repeats a key, and tells the package", async () => {
      const merge = {
         ...LINEAGE,
         mergeKeys: ["id"],
         strategy: "merge",
      } as unknown as typeof LINEAGE;
      const noted: [string, boolean][] = [];
      const step = {
         mode: "delta",
         start: { malloyType: "date", value: "2024-06-01" },
         end: { malloyType: "date", value: "2024-07-01" },
         coveredThrough: { malloyType: "date", value: "2024-07-01" },
         statements: ["MERGE INTO t USING s ON true"],
      } as never;
      const withNotes = {
         ...context(),
         noteMergeKeyUniqueness: (id: string, unique: boolean) =>
            noted.push([id, unique]),
      };
      const applied = await applyIncrementalStep({
         context: withNotes,
         lineage: merge,
         step,
         target: {
            ...target(merge),
            runner: async () => {
               throw new Error(
                  `invalid input syntax for type integer: "${MERGE_KEY_GUARD_MESSAGE}"`,
               );
            },
         },
         sourceName: "daily",
      });
      expect(applied).toEqual({ applied: false });
      expect(await counted("seed", "merge_key_not_unique")).toBe(1);
      expect(noted).toEqual([["sid", false]]);
      const logged = warn
         .getCalls()
         .find(
            (c) =>
               (c.args[1] as { code?: string })?.code ===
               "persist-incremental-merge-key-not-unique",
         );
      expect(logged?.args[1]).toMatchObject({
         sourceName: "daily",
         mergeKeys: ["id"],
      });

      // A delta that commits finds the key unique, which clears the warning.
      await applyIncrementalStep({
         context: withNotes,
         lineage: merge,
         step,
         target: { ...target(merge), runner: async () => ({ rows: [] }) },
         sourceName: "daily",
      });
      expect(noted).toEqual([
         ["sid", false],
         ["sid", true],
      ]);
   });

   it.each([
      ["repeats a key", [{ repeated: 1 }], false],
      ["holds each key once", [], true],
   ] as const)(
      "records a merge source's boundary after a rebuild only when the table holds each key once: %s",
      async (_label, repeatedRows, unique) => {
         const merge = {
            ...LINEAGE,
            mergeKeys: ["id"],
            scopeColumns: ["org"],
            strategy: "merge",
         } as unknown as typeof LINEAGE;
         const noted: [string, boolean][] = [];
         const upserted: unknown[] = [];
         const probes: string[] = [];
         const bound = await advanceLedgerAfterSeed({
            context: {
               ...context({
                  upsertIncrementalLedgerEntry: async (e: unknown) => {
                     upserted.push(e);
                     return e as never;
                  },
               }),
               noteMergeKeyUniqueness: (id: string, u: boolean) =>
                  noted.push([id, u]),
            },
            lineage: merge,
            quotedTablePath: `"analytics"."daily"`,
            dialect: "postgres",
            runner: async (sql) => {
               probes.push(sql);
               if (sql.includes("HAVING COUNT(*) > 1")) {
                  return { rows: [...repeatedRows] };
               }
               return { rows: [{ watermark_max: "2024-06-30" }] };
            },
         });
         expect(probes[0]).toContain(
            'GROUP BY "id", "org" HAVING COUNT(*) > 1',
         );
         expect(noted).toEqual([["sid", unique]]);
         expect(bound === undefined).toBe(!unique);
         expect(upserted.length).toBe(unique ? 1 : 0);
      },
   );

   it("fails, rather than rebuilding, when a pair delta fails for any other reason", async () => {
      const pair = {
         ...LINEAGE,
         pair: { parentKey: "id", parentRow: "__parent_row" },
      } as unknown as typeof LINEAGE;
      const failing: DeltaTarget = {
         ...target(pair),
         runner: async () => {
            throw new Error("disk full");
         },
      };
      await expect(
         applyIncrementalStep({
            context: context(),
            lineage: pair,
            step: {
               mode: "delta",
               start: { malloyType: "date", value: "2024-06-01" },
               end: { malloyType: "date", value: "2024-07-01" },
               coveredThrough: { malloyType: "date", value: "2024-07-01" },
               statements: ["DELETE FROM t", "INSERT INTO t SELECT 1"],
            } as never,
            target: failing,
            sourceName: "orders_all",
         }),
      ).rejects.toThrow("disk full");
      expect(await counted("seed", "pair_parent_row")).toBe(0);
   });

   it("rebuilds a chained stored source with chained_storage, clearing its boundary first", async () => {
      const deleted: unknown[] = [];
      const declaration = {
         refresh: "incremental",
         incremental: true,
         declaredWatermark: true,
         declaredMergeKey: false,
         watermark: {
            name: "order_date",
            kind: "dimension",
            malloyType: "date",
         },
         watermarkOrderable: true,
         mergeKeys: [],
         scopeColumns: [],
         scopeIncomplete: false,
         watermarkInMergeKeys: false,
         strategy: "range_replace",
         malformed: [],
         unknownKeys: [],
         calculateFields: [],
         outputColumns: ["order_date", "revenue"],
      } as unknown as IncrementalDeclaration;
      const incremental = {
         ...context({
            deleteIncrementalLedgerEntry: async (_env: string, l: unknown) => {
               deleted.push(l);
            },
         }),
         declarations: { "daily@m": declaration },
      } as IncrementalRunContext;

      const strategy = new StorageBuildStrategy() as unknown as {
         refreshOf(
            params: unknown,
            destination: unknown,
         ): Promise<{ incrementalRefresh: unknown; refresh: unknown }>;
      };
      const { incrementalRefresh, refresh } = await strategy.refreshOf(
         {
            persistSource: {
               name: "daily",
               sourceID: "daily@m",
               dialectName: "postgres",
               connectionName: "wh",
            },
            instruction: { physicalTableName: "daily_v1", destination: "lake" },
            dependsOnStorageUpstream: true,
            incremental,
            contentSourceEntityId: "addr-1",
            buildSQL: "SELECT 1",
         },
         { name: "lake", type: "ducklake" },
      );

      expect(incrementalRefresh).toBeDefined();
      // No delta: the source rebuilds in full.
      expect(refresh).toBeUndefined();
      expect(deleted).toHaveLength(1);
      expect(deleted[0]).toMatchObject({
         physicalTableName: "daily_v1",
         storageDestinationName: "lake",
      });
      expect(await counted("seed", "chained_storage")).toBe(1);
   });
});
