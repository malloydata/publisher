// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { ManifestEntry } from "../domain/types";
import type {
   ExtensionParent,
   IncrementalLineage,
   RecordedBoundary,
} from "./incremental_apply";
import {
   boundParentReads,
   coordinateWithParent,
   ledgerParentOf,
   parentCoverage,
} from "./incremental_upstream";

/**
 * An incremental extension table follows its parent's table (extension
 * chains, case 2): it starts no later than the parent's delta, ends no later
 * than the parent's coverage, and deltas only over the build of the parent's
 * table it last read.
 */

const PARENT_ENTRY: ManifestEntry = {
   sourceEntityId: "parent-addr",
   physicalTableName: "parent_t",
   builtAt: "2024-06-01T00:00:10.000Z",
   dataAsOf: "2024-06-01T00:00:00.000Z",
   ledger: {
      connectionName: "wh",
      physicalTableName: "parent_t",
      coveredThrough: "2024-06-01 00:00:00",
      coveredThroughType: "timestamp",
      watermark: "ts",
      strategy: "range_replace",
      sourceEntityId: "parent-addr",
   },
} as ManifestEntry;

const parentOf = (over: Partial<ExtensionParent> = {}): ExtensionParent => ({
   address: "parent-addr",
   name: "events",
   coordinate: "delta",
   parentIncremental: true,
   entry: PARENT_ENTRY,
   builtThisRun: false,
   ...over,
});

const lineageOf = (parent?: ExtensionParent): IncrementalLineage => ({
   physicalTableName: "ext_t",
   connectionName: "wh",
   sourceEntityId: "ext-addr",
   watermarkName: "ts",
   watermarkType: "timestamp",
   mergeKeys: [],
   scopeColumns: [],
   strategy: "range_replace",
   ...(parent ? { parent } : {}),
});

const RECORDED: RecordedBoundary = {
   sourceEntityId: "ext-addr",
   coveredThroughValue: "2024-06-01 00:00:00",
   coveredThroughType: "timestamp",
   watermarkDimension: "ts",
   mergeKeyDimensions: [],
   derivedStrategy: "range_replace",
   physicalTableName: "ext_t",
   connectionName: "wh",
};

/** The read an extension built right after the parent's previous build records. */
const READ_PREVIOUS = {
   sourceEntityId: "parent-addr",
   builtAt: PARENT_ENTRY.builtAt,
   coveredThrough: "2024-06-01 00:00:00",
   coveredThroughType: "timestamp",
};

describe("coordinateWithParent", () => {
   it("follows a parent refreshed by delta from the build it last read", () => {
      const parent = parentOf({ builtThisRun: true });
      expect(
         coordinateWithParent({
            lineage: lineageOf(parent),
            recorded: RECORDED,
            read: READ_PREVIOUS,
            step: {
               mode: "delta",
               prior: { malloyType: "timestamp", value: "2024-06-01 00:00:00" },
               start: { malloyType: "timestamp", value: "2024-05-30 00:00:00" },
            },
         }),
      ).toEqual({
         startAtMost: { malloyType: "timestamp", value: "2024-05-30 00:00:00" },
      });
   });

   it("reseeds an extension that missed a parent build", () => {
      const bounds = coordinateWithParent({
         lineage: lineageOf(parentOf({ builtThisRun: true })),
         recorded: RECORDED,
         // It last read the build before the parent's prior one.
         read: { ...READ_PREVIOUS, coveredThrough: "2024-05-01 00:00:00" },
         step: {
            mode: "delta",
            prior: { malloyType: "timestamp", value: "2024-06-01 00:00:00" },
            start: { malloyType: "timestamp", value: "2024-05-30 00:00:00" },
         },
      });
      expect(bounds?.reseed?.reasonCode).toBe("upstream_generation_skipped");
      expect(bounds?.reseed?.reason).toContain("is not the one");
   });

   it("reseeds, saying so, when no record names the build it last read", () => {
      const bounds = coordinateWithParent({
         lineage: lineageOf(parentOf({ builtThisRun: true })),
         recorded: RECORDED,
         read: undefined,
         step: { mode: "skip" },
      });
      expect(bounds?.reseed?.reasonCode).toBe("upstream_generation_skipped");
      expect(bounds?.reseed?.reason).toContain("no record says");
   });

   it("deltas over a parent seeded this run, a fresh build from raw", () => {
      expect(
         coordinateWithParent({
            lineage: lineageOf(parentOf({ builtThisRun: true })),
            recorded: RECORDED,
            read: undefined,
            step: { mode: "seed" },
         }),
      ).toEqual({});
   });

   it("caps a time range at a carried parent's coverage", () => {
      expect(
         coordinateWithParent({
            lineage: lineageOf(parentOf()),
            recorded: RECORDED,
            read: READ_PREVIOUS,
            step: undefined,
         }),
      ).toEqual({
         endAtMost: { malloyType: "timestamp", value: "2024-06-01 00:00:00" },
      });
   });

   it("over a full parent: only the cap, no generation to prove", () => {
      expect(
         coordinateWithParent({
            lineage: lineageOf(
               parentOf({
                  parentIncremental: false,
                  entry: { ...PARENT_ENTRY, ledger: undefined },
               }),
            ),
            recorded: RECORDED,
            read: undefined,
            step: undefined,
         }),
      ).toEqual({
         endAtMost: { malloyType: "timestamp", value: "2024-06-01 00:00:00" },
      });
   });

   it("on different watermarks: reseeds when the parent changed, else follows", () => {
      const parent = parentOf({
         builtThisRun: true,
         coordinate: "reseed_on_parent_change",
      });
      const prior = { malloyType: "timestamp", value: "2024-06-01 00:00:00" };
      const changed = coordinateWithParent({
         lineage: lineageOf(parent),
         recorded: RECORDED,
         read: READ_PREVIOUS,
         step: { mode: "delta", prior, start: prior },
      });
      expect(changed?.reseed?.reasonCode).toBe("upstream_watermark_differs");
      // The parent skipped: its table is the one the extension last read.
      expect(
         coordinateWithParent({
            lineage: lineageOf(parent),
            recorded: RECORDED,
            read: READ_PREVIOUS,
            step: { mode: "skip", prior },
         }),
      ).toEqual({});
   });

   it("leaves a source with no parent, or no boundary, alone", () => {
      expect(
         coordinateWithParent({
            lineage: lineageOf(),
            recorded: RECORDED,
            read: undefined,
            step: undefined,
         }),
      ).toBeUndefined();
      expect(
         coordinateWithParent({
            lineage: lineageOf(parentOf({ builtThisRun: true })),
            recorded: null,
            read: undefined,
            step: { mode: "delta" },
         }),
      ).toEqual({});
   });
});

describe("parentCoverage", () => {
   it("reads the boundary, else dataAsOf, in the extension's type", () => {
      expect(parentCoverage(PARENT_ENTRY, "date")).toEqual({
         malloyType: "date",
         value: "2024-06-01",
      });
      expect(
         parentCoverage({ ...PARENT_ENTRY, ledger: undefined }, "timestamp"),
      ).toEqual({ malloyType: "timestamp", value: "2024-06-01 00:00:00" });
      expect(parentCoverage(PARENT_ENTRY, "number")).toBeUndefined();
   });
});

describe("the parent a boundary records", () => {
   it("names the parent's build as the run holds it", () => {
      expect(ledgerParentOf(lineageOf(parentOf()))).toEqual(READ_PREVIOUS);
      expect(ledgerParentOf(lineageOf())).toBeUndefined();
   });

   it("is read back from a bound entry's ledger, else its builtFrom", () => {
      const reads = boundParentReads([
         [
            "ext-addr",
            {
               ledger: { ...PARENT_ENTRY.ledger!, parent: READ_PREVIOUS },
            },
         ],
         [
            "other",
            {
               builtFrom: [
                  {
                     sourceEntityId: "parent-addr",
                     physicalTableName: "parent_t",
                     builtAt: "b",
                     coveredThrough: "c",
                  },
               ],
            },
         ],
      ]);
      expect(reads.get("ext-addr")).toEqual([READ_PREVIOUS]);
      expect(reads.get("other")).toEqual([
         { sourceEntityId: "parent-addr", builtAt: "b", coveredThrough: "c" },
      ]);
   });
});
