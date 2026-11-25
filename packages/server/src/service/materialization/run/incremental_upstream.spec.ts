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
      expect(bounds?.reseed?.reason).toContain(
         "is not one the parent's table was reached from",
      );
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

describe("an extension refreshed after its parent's own calls", () => {
   const ts = (value: string) => ({ malloyType: "timestamp", value });
   const adv = (from: string, advancedBy: string, start: string) => ({
      from,
      advancedBy,
      start,
      startType: "timestamp",
   });
   /** The parent as carried after deltas r2 and r3 since its seed r1. */
   const carried = (ledger: Record<string, unknown>): ExtensionParent =>
      parentOf({
         entry: {
            ...PARENT_ENTRY,
            builtAt: "2024-06-03T00:00:10.000Z",
            ledger: {
               ...PARENT_ENTRY.ledger!,
               coveredThrough: "2024-06-03 00:00:00",
               ...ledger,
            },
         } as ManifestEntry,
      });
   const CHAIN = {
      advancedBy: "r3",
      seededBy: "r1",
      advances: [
         adv("r1", "r2", "2024-05-25 00:00:00"),
         adv("r2", "r3", "2024-05-20 00:00:00"),
      ],
   };
   /** The extension read the parent's r1 build. */
   const READ_R1 = {
      sourceEntityId: "parent-addr",
      advancedBy: "r1",
      seededBy: "r1",
      builtAt: PARENT_ENTRY.builtAt,
      coveredThrough: "2024-06-01 00:00:00",
      coveredThroughType: "timestamp",
   };
   const coordinate = (
      parent: ExtensionParent,
      read: typeof READ_R1 | undefined,
      step?: Parameters<typeof coordinateWithParent>[0]["step"],
   ) =>
      coordinateWithParent({
         lineage: lineageOf(parent),
         recorded: RECORDED,
         read,
         step,
      });

   it("deltas from the lowest start of the parent's deltas since the build it read", () => {
      expect(coordinate(carried(CHAIN), READ_R1)).toEqual({
         endAtMost: ts("2024-06-03 00:00:00"),
         startAtMost: ts("2024-05-20 00:00:00"),
      });
      // Read r2: only r3's delta is after it.
      expect(
         coordinate(carried(CHAIN), { ...READ_R1, advancedBy: "r2" })
            ?.startAtMost,
      ).toEqual(ts("2024-05-20 00:00:00"));
   });

   it("in a run that refreshes the parent again, adds that delta's start", () => {
      const parent = parentOf({ builtThisRun: true });
      const bounds = coordinate(parent, READ_R1, {
         mode: "delta",
         priorAdvancedBy: "r3",
         prior: ts("2024-06-03 00:00:00"),
         start: ts("2024-05-27 00:00:00"),
         priorChain: { seededBy: "r1", advances: CHAIN.advances },
      });
      expect(bounds).toEqual({ startAtMost: ts("2024-05-20 00:00:00") });
   });

   it("accepts the build it read by builtAt on the step path", () => {
      const parent = parentOf({ builtThisRun: true });
      const read = {
         sourceEntityId: "parent-addr",
         builtAt: "2024-06-01T00:00:10.000Z",
      } as typeof READ_R1;
      expect(
         coordinate(parent, read, {
            mode: "delta",
            priorAdvancedBy: "r1",
            prior: ts("2024-06-01 00:00:00"),
            start: ts("2024-05-30 00:00:00"),
            priorBuiltAt: "2024-06-01T00:00:10.000Z",
         }),
      ).toEqual({ startAtMost: ts("2024-05-30 00:00:00") });
   });

   it("reseeds when the parent was rebuilt in full since", () => {
      const reseeded = carried({
         advancedBy: "r4",
         seededBy: "r4",
         advances: [],
      });
      expect(coordinate(reseeded, READ_R1)?.reseed?.reasonCode).toBe(
         "upstream_generation_skipped",
      );
   });

   it("reseeds, naming the cap, when more deltas followed than the ledger lists", () => {
      const advances = Array.from({ length: 8 }, (_, i) =>
         adv(`r${i + 2}`, `r${i + 3}`, "2024-05-20 00:00:00"),
      );
      const bounds = coordinate(
         carried({ advancedBy: "r10", seededBy: "r1", advances }),
         READ_R1,
      );
      expect(bounds?.reseed?.reasonCode).toBe("upstream_generation_skipped");
      expect(bounds?.reseed?.reason).toContain("more than 8 times");
   });

   it("reseeds when any record is missing, broken or of another type", () => {
      const cases: [Record<string, unknown>, typeof READ_R1][] = [
         // An entry from before these keys.
         [{ advancedBy: "r3" }, READ_R1],
         // A read from before them.
         [CHAIN, { ...READ_R1, seededBy: undefined } as never],
         // A gap in the list.
         [
            {
               ...CHAIN,
               advances: [CHAIN.advances[0], adv("rX", "r3", "2024-05-20")],
            },
            READ_R1,
         ],
         // The list does not end at the build the parent holds.
         [{ ...CHAIN, advancedBy: "r9" }, READ_R1],
         // A start in another watermark type.
         [
            {
               ...CHAIN,
               advances: [
                  CHAIN.advances[0],
                  { ...CHAIN.advances[1], startType: "date" },
               ],
            },
            READ_R1,
         ],
         // A malformed list.
         [{ ...CHAIN, advances: [{ from: "r1" }] }, READ_R1],
      ];
      for (const [ledger, read] of cases) {
         expect([
            ledger,
            coordinate(carried(ledger), read)?.reseed?.reasonCode,
         ]).toEqual([ledger, "upstream_generation_skipped"]);
      }
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
