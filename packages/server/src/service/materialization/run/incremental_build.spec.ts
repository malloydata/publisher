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
   incrementalLineage,
   indexCallerLedger,
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
