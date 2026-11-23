// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The run-level predicate that decides whether a build touches the delta path at
// all. `incrementalLineage` is read from more than one place — it gates the
// skip-if-unchanged exemption in deriveSelfInstructions AND the dispatch in
// buildOneSource, and the two must never disagree — so it is pinned here rather
// than only through the callers.
import { describe, expect, it } from "bun:test";
import { makeDigest } from "@malloydata/malloy";
import {
   ledgerExtensionKeys,
   incrementalLineage,
   indexCallerLedger,
   ledgerFields,
   planSourceRefresh,
   type IncrementalRunContext,
} from "./incremental_build";
import type { DeltaTarget } from "./incremental_apply";
import type { IncrementalDeclaration } from "../domain/incremental";

const DIMENSION = { kind: "dimension" as const, malloyType: "date" };

function declaration(
   overrides: Partial<IncrementalDeclaration> = {},
): IncrementalDeclaration {
   return {
      refresh: "incremental",
      incremental: true,
      declaredWatermark: true,
      declaredMergeKey: false,
      watermark: { name: "order_date", ...DIMENSION },
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
      ...overrides,
   } as IncrementalDeclaration;
}

function lineage(
   overrides: {
      declaration?: IncrementalDeclaration | undefined;
      dialect?: string;
      targetDialect?: string;
      storageDestinationName?: string;
   } = {},
) {
   return incrementalLineage({
      declaration:
         "declaration" in overrides ? overrides.declaration : declaration(),
      dialect: overrides.dialect ?? "postgres",
      targetDialect: overrides.targetDialect ?? overrides.dialect ?? "postgres",
      physicalTableName: "orders_v1",
      connectionName: "wh",
      storageDestinationName: overrides.storageDestinationName,
      sourceEntityId: "addr-1",
   });
}

describe("incrementalLineage", () => {
   it("describes the source a delta would advance", () => {
      expect(
         lineage({
            declaration: declaration({
               declaredMergeKey: true,
               mergeKeys: [
                  { name: "order_id", ...DIMENSION, malloyType: "number" },
                  { name: "region", ...DIMENSION, malloyType: "string" },
               ],
               strategy: "merge",
            }),
         }),
      ).toEqual({
         physicalTableName: "orders_v1",
         connectionName: "wh",
         sourceEntityId: "addr-1",
         watermarkName: "order_date",
         watermarkType: "date",
         mergeKeys: ["order_id", "region"],
         scopeColumns: [],
         strategy: "merge",
      });
   });

   it("carries a #(materialize) source's rolling window, for the refresh's delete", () => {
      const window = [
         {
            field: "order_ts",
            op: ">=" as const,
            offset: { sign: -1 as const, amount: 30, unit: "day" as const },
         },
      ];
      expect(
         lineage({ declaration: declaration({ rollingWindow: window }) })
            ?.rollingWindow,
      ).toEqual(window);
      expect(lineage()).not.toHaveProperty("rollingWindow");
   });

   // Every case below means "build this the ordinary way and do not touch the
   // ledger". The first two are the overwhelming majority of sources; the rest
   // are declarations the publish gate rejects, so reaching a build with one
   // means the package predates the gates.
   it("declines a source with no declaration", () => {
      expect(lineage({ declaration: undefined })).toBeUndefined();
   });

   it("declines a source that is not incremental", () => {
      expect(
         lineage({ declaration: declaration({ incremental: false }) }),
      ).toBeUndefined();
   });

   it("declines a dialect off the allowlist", () => {
      expect(lineage({ dialect: "duckdb" })).toBeUndefined();
   });

   it("describes a storage= build, whose table lives in the destination", () => {
      // The destination is part of the boundary's table identity, so the same
      // source materialized in the database tier and stored yields two distinct lineages.
      const stored = lineage({
         targetDialect: "duckdb",
         storageDestinationName: "lake",
      });
      expect(stored?.storageDestinationName).toBe("lake");
      expect(lineage()?.storageDestinationName).toBeUndefined();
   });

   it("describes a pair table, replaced by its parent key in DuckDB", () => {
      const pair = declaration({
         declaredMergeKey: true,
         mergeKeys: [{ name: "id", ...DIMENSION, malloyType: "number" }],
         strategy: "merge",
         pair: { parentKey: "id", parentRow: "__parent_row" },
      });
      expect(
         lineage({ declaration: pair, targetDialect: "duckdb" })?.pair,
      ).toEqual({ parentKey: "id", parentRow: "__parent_row" });
      // Anywhere else, or keyed on anything but the parent, it seeds.
      expect(lineage({ declaration: pair })).toBeUndefined();
      expect(
         lineage({
            declaration: { ...pair, mergeKeys: [] },
            targetDialect: "duckdb",
         }),
      ).toBeUndefined();
      expect(lineage()).not.toHaveProperty("pair");
   });

   it("declines a target engine whose transactional apply is unproven", () => {
      expect(lineage({ targetDialect: "mysql" })).toBeUndefined();
   });

   it("declines a watermark that is not a materialized dimension", () => {
      expect(
         lineage({ declaration: declaration({ watermark: undefined }) }),
      ).toBeUndefined();
      expect(
         lineage({
            declaration: declaration({
               watermark: {
                  name: "revenue",
                  kind: "aggregate",
                  malloyType: "number",
               },
            }),
         }),
      ).toBeUndefined();
      expect(
         lineage({
            declaration: declaration({
               watermark: {
                  name: "flag",
                  kind: "dimension",
                  malloyType: "boolean",
               },
               watermarkOrderable: false,
            }),
         }),
      ).toBeUndefined();
   });

   it("declines when a merge key is not a materialized dimension", () => {
      // Partial row identity is worse than none: the merge would match on fewer
      // columns than the author declared and quietly overwrite the wrong rows.
      expect(
         lineage({
            declaration: declaration({
               declaredMergeKey: true,
               mergeKeys: [
                  { name: "region", ...DIMENSION, malloyType: "string" },
                  { name: "revenue", kind: "aggregate", malloyType: "number" },
               ],
               strategy: "merge",
            }),
         }),
      ).toBeUndefined();
   });
});

// The premise behind not restoring the bounded delta for a table an earlier
// `partition=` build laid out: the ledger is keyed by table, and its entry
// records the address the boundary was measured under. That address folded the
// layout in (`-- partitioned by: …`), which no source has any more, so the
// entry can never match: standalone the source seeds, and a caller-supplied
// entry fails the run until it is reseeded.
describe("planSourceRefresh: a boundary measured under a partition= address", () => {
   const SQL = "SELECT d, n FROM orders";
   const lineage = {
      physicalTableName: "daily",
      connectionName: "wh",
      storageDestinationName: "lake",
      sourceEntityId: makeDigest("conn-digest", SQL),
      watermarkName: "d",
      watermarkType: "date",
      mergeKeys: [] as string[],
      scopeColumns: [] as string[],
      strategy: "range_replace" as const,
   };
   const measured = {
      sourceEntityId: makeDigest("conn-digest", `${SQL}\n-- partitioned by: d`),
      coveredThroughValue: "2026-02-01",
      coveredThroughType: "date",
      watermarkDimension: "d",
      mergeKeyDimensions: [] as string[],
      derivedStrategy: "range_replace" as const,
      physicalTableName: "daily",
      connectionName: "wh",
      storageDestinationName: "lake",
   };
   const context = (overrides: Partial<IncrementalRunContext> = {}) =>
      ({
         environmentId: "env",
         packageName: "pkg",
         materializationId: "run",
         forceRefresh: false,
         now: new Date("2026-03-01T00:00:00Z"),
         declarations: {},
         ledger: {
            getIncrementalLedgerEntry: async () => measured,
         },
         ...overrides,
      }) as unknown as IncrementalRunContext;
   const target = {
      dialect: "duckdb",
      quotedTablePath: '"lake"."daily"',
      logicalTablePath: "lake.daily",
   } as unknown as DeltaTarget;

   it("seeds a standalone run, as a changed definition", async () => {
      const step = await planSourceRefresh({
         context: context(),
         lineage,
         target,
         columns: ["d", "n"],
      });
      expect(step).toMatchObject({
         mode: "seed",
         reasonCode: "lineage_changed",
      });
   });

   it("fails a run whose caller supplied that entry, until it is reseeded", async () => {
      const callerLedger = indexCallerLedger([
         {
            sourceEntityId: measured.sourceEntityId,
            coveredThrough: measured.coveredThroughValue,
            coveredThroughType: measured.coveredThroughType,
            watermark: measured.watermarkDimension,
            mergeKeys: [],
            strategy: measured.derivedStrategy,
            physicalTableName: measured.physicalTableName,
            connectionName: measured.connectionName,
            storageDestinationName: measured.storageDestinationName,
         } as never,
      ]);
      await expect(
         planSourceRefresh({
            context: context({ callerLedger }),
            lineage,
            target,
            columns: ["d", "n"],
         }),
      ).rejects.toThrow(/measured under a different definition/);
      // Reseeded, it builds.
      expect(
         await planSourceRefresh({
            context: context({ callerLedger }),
            lineage,
            target,
            columns: ["d", "n"],
            reseed: true,
         }),
      ).toMatchObject({ mode: "seed", reasonCode: "forced" });
   });
});

describe("indexCallerLedger and ledgerFields: the entry is opaque to hosts", () => {
   const ENTRY = {
      connectionName: "wh",
      physicalTableName: "orders_v1",
      coveredThrough: "2024-06-20",
      coveredThroughType: "date",
      watermark: "order_date",
      strategy: "range_replace" as const,
      sourceEntityId: "addr",
   };

   it("reads a missing mergeKeys as empty, the same as an empty list", () => {
      const [absent] = [...indexCallerLedger([ENTRY]).values()];
      const [empty] = [
         ...indexCallerLedger([{ ...ENTRY, mergeKeys: [] }]).values(),
      ];
      expect(absent?.mergeKeyDimensions).toEqual([]);
      expect(empty).toEqual(absent);
      expect(absent?.extensions).toBeUndefined();
   });

   it("carries keys it does not know as extensions, untouched", () => {
      const parent = { sourceEntityId: "p1", coveredThrough: "2024-06-19" };
      const [boundary] = [
         ...indexCallerLedger([
            { ...ENTRY, mergeKeys: ["id"], parentGeneration: parent, n: 1 },
         ]).values(),
      ];
      expect(boundary?.extensions).toEqual({ parentGeneration: parent, n: 1 });
      // Known fields are read as before.
      expect(boundary?.mergeKeyDimensions).toEqual(["id"]);
      expect(boundary?.coveredThroughValue).toBe("2024-06-20");
   });

   it("reports extensions on the next entry, a known key always winning", () => {
      const lineage = {
         connectionName: "wh",
         physicalTableName: "orders_v1",
         sourceEntityId: "addr",
         watermarkName: "order_date",
         watermarkType: "date",
         mergeKeys: [] as string[],
         scopeColumns: [] as string[],
         strategy: "range_replace" as const,
      };
      const bound = { value: "2024-07-01", malloyType: "date" };
      expect(
         ledgerFields(lineage as never, bound as never, {
            parentGeneration: { g: 2 },
            coveredThrough: "spoofed",
         }),
      ).toEqual({
         ledger: {
            ...ENTRY,
            coveredThrough: "2024-07-01",
            parentGeneration: { g: 2 },
         },
      });
      // Without extensions, the entry is exactly the known fields.
      expect(ledgerFields(lineage as never, bound as never)).toEqual({
         ledger: { ...ENTRY, coveredThrough: "2024-07-01" },
      });
   });

   describe("ledgerExtensionKeys", () => {
      const lineage = {
         connectionName: "wh",
         physicalTableName: "orders_v1",
         sourceEntityId: "addr",
         watermarkName: "order_date",
         watermarkType: "date",
         mergeKeys: [] as string[],
         scopeColumns: [] as string[],
         strategy: "range_replace" as const,
      };
      const bound = { value: "2024-07-01", malloyType: "date" };
      function refresh(
         ledger: Record<string, unknown>[] | undefined,
         forceRefresh = false,
      ) {
         return {
            context: {
               forceRefresh,
               ...(ledger
                  ? { callerLedger: indexCallerLedger(ledger as never) }
                  : {}),
            } as unknown as IncrementalRunContext,
            lineage: lineage as never,
         };
      }

      describe("the history since the last full rebuild (seededBy, advances)", () => {
         const withStep = (step: Record<string, unknown> | undefined) => ({
            context: {
               forceRefresh: false,
               materializationId: "run-now",
               parentSteps: new Map(step ? [["addr", step]] : []),
            } as unknown as IncrementalRunContext,
            lineage: lineage as never,
         });
         const adv = (from: string, advancedBy: string) => ({
            from,
            advancedBy,
            start: "2024-06-01",
            startType: "date",
         });
         const start = { malloyType: "date", value: "2024-06-20" };

         it("starts over on a full rebuild", () => {
            expect(
               ledgerExtensionKeys(
                  withStep(undefined),
                  undefined,
                  "run-now",
                  "full",
               ),
            ).toEqual({
               advancedBy: "run-now",
               seededBy: "run-now",
               advances: [],
            });
         });

         it("appends a delta to the history it started from", () => {
            const keys = ledgerExtensionKeys(
               withStep({
                  mode: "delta",
                  priorAdvancedBy: "r2",
                  start,
                  priorChain: { seededBy: "r1", advances: [adv("r1", "r2")] },
               }),
               undefined,
               "run-now",
               "delta",
            );
            expect(keys).toEqual({
               advancedBy: "run-now",
               seededBy: "r1",
               advances: [
                  adv("r1", "r2"),
                  {
                     from: "r2",
                     advancedBy: "run-now",
                     start: "2024-06-20",
                     startType: "date",
                  },
               ],
            });
         });

         it("keeps the newest eight", () => {
            const advances = Array.from({ length: 8 }, (_, i) =>
               adv(`r${i + 1}`, `r${i + 2}`),
            );
            const keys = ledgerExtensionKeys(
               withStep({
                  mode: "delta",
                  priorAdvancedBy: "r9",
                  start,
                  priorChain: { seededBy: "r1", advances },
               }),
               undefined,
               "run-now",
               "delta",
            ) as { seededBy: string; advances: { from: string }[] };
            expect(keys.seededBy).toBe("r1");
            expect(keys.advances.map((a) => a.from)).toEqual([
               "r2",
               "r3",
               "r4",
               "r5",
               "r6",
               "r7",
               "r8",
               "r9",
            ]);
         });

         it("anchors a new history at the build a delta started from when none, or a broken one, is recorded", () => {
            for (const priorChain of [
               undefined,
               { seededBy: "r1", advances: [adv("r1", "rX")] },
            ]) {
               expect(
                  ledgerExtensionKeys(
                     withStep({
                        mode: "delta",
                        priorAdvancedBy: "r2",
                        start,
                        ...(priorChain ? { priorChain } : {}),
                     }),
                     undefined,
                     "run-now",
                     "delta",
                  ),
               ).toEqual({
                  advancedBy: "run-now",
                  seededBy: "r2",
                  advances: [
                     {
                        from: "r2",
                        advancedBy: "run-now",
                        start: "2024-06-20",
                        startType: "date",
                     },
                  ],
               });
            }
         });

         it("keeps it through a refresh that applied nothing, and never carries an echoed one", () => {
            const priorChain = { seededBy: "r1", advances: [adv("r1", "r2")] };
            expect(
               ledgerExtensionKeys(
                  withStep({ mode: "skip", priorAdvancedBy: "r2", priorChain }),
                  undefined,
                  "r2",
                  "none",
               ),
            ).toEqual({ advancedBy: "r2", ...priorChain });
            const echoed = refresh([
               { ...ENTRY, seededBy: "forged", advances: [adv("a", "b")] },
            ]);
            expect(
               ledgerExtensionKeys(echoed, undefined, undefined, undefined),
            ).toBeUndefined();
         });
      });

      it("carries the echoed entry's unknown keys for the source's table", () => {
         const r = refresh([{ ...ENTRY, future: "g1" }]);
         expect(
            ledgerExtensionKeys(r, undefined, undefined, undefined),
         ).toEqual({
            future: "g1",
         });
      });

      it("amends the carried keys with the ones this run computes, never an echoed one", () => {
         const echoed = {
            ...ENTRY,
            future: "g1",
            advancedBy: "old-run",
            parent: { sourceEntityId: "p", advancedBy: "old-run" },
         };
         const r = refresh([echoed]);
         expect(
            ledgerFields(
               lineage as never,
               bound as never,
               ledgerExtensionKeys(r, undefined, "this-run", undefined),
            ),
         ).toEqual({
            ledger: {
               ...ENTRY,
               coveredThrough: "2024-07-01",
               future: "g1",
               advancedBy: "this-run",
            },
         });
         // A computed key the run has no value for is dropped, not carried.
         expect(
            ledgerExtensionKeys(r, undefined, undefined, undefined),
         ).toEqual({
            future: "g1",
         });
         // The run's index is untouched, for any other reader.
         expect(
            [...(r.context.callerLedger?.values() ?? [])][0]?.extensions,
         ).toEqual({
            future: "g1",
            advancedBy: "old-run",
            parent: { sourceEntityId: "p", advancedBy: "old-run" },
         });
      });

      it("carries nothing for a reseed, a forced run, the local store, or another table", () => {
         const echoed = [{ ...ENTRY, future: "g1" }];
         expect(
            ledgerExtensionKeys(refresh(echoed), true, undefined, undefined),
         ).toBeUndefined();
         expect(
            ledgerExtensionKeys(
               refresh(echoed, true),
               undefined,
               undefined,
               undefined,
            ),
         ).toBeUndefined();
         expect(
            ledgerExtensionKeys(
               refresh(undefined),
               undefined,
               undefined,
               undefined,
            ),
         ).toBeUndefined();
         expect(
            ledgerExtensionKeys(
               refresh([{ ...echoed[0], physicalTableName: "other" }]),
               undefined,
               undefined,
               undefined,
            ),
         ).toBeUndefined();
         expect(
            ledgerExtensionKeys(undefined, undefined, undefined, undefined),
         ).toBeUndefined();
      });
   });
});
