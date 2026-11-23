// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An incremental extension table's refresh over its parent's table
 * (extension chains, case 2): what the parent's step in this run, and the
 * build of the parent's table the extension last read, allow its own step
 * ({@link coordinateWithParent}), and which build of the parent's table a
 * boundary is reached over ({@link ledgerParentOf}).
 *
 * The parent's table is the extension's raw. Its own watermark, lookback,
 * strategy and rolling windows are unchanged; two things are added because a
 * stored parent moves under it in ways a warehouse table does not:
 *  - its delta starts no later than the parent's own delta in the run, and a
 *    time watermark's range ends no later than the parent's coverage;
 *  - it deltas only over the build of the parent's table it last read, or
 *    one refreshed in this run from exactly that build; otherwise it
 *    rebuilds in full over the parent's table (`upstream_generation_skipped`).
 *
 * Pure.
 */

import type { ManifestEntry } from "../domain/types";
import {
   canonicalBoundValue,
   compareBounds,
   type ExtensionParent,
   type IncrementalLineage,
   type LedgerParent,
   type RecordedBoundary,
   type UpstreamBounds,
   type WatermarkBound,
} from "./incremental_apply";

/**
 * What one incremental source's step was in this run, kept for the
 * extensions over it: the boundary it advanced from, and where its delta
 * started.
 */
export interface ParentStep {
   mode: "seed" | "skip" | "delta";
   /** The boundary recorded before this run's step, when there was one. */
   prior?: WatermarkBound;
   /** The run that last changed the table before this step (`advancedBy`). */
   priorAdvancedBy?: string;
   /** A delta's range start. */
   start?: WatermarkBound;
}

export type { UnprovenGeneration } from "../domain/incremental";

/**
 * The bounds the parent allows an extension's step, or undefined for a
 * source that extends no incremental-coordinated parent. `read` is which
 * build of the parent's table the extension's recorded boundary was reached
 * over: from the run's caller ledger entry, else (standalone) the bound
 * manifest; undefined when neither records it.
 */
export function coordinateWithParent(args: {
   lineage: IncrementalLineage;
   recorded: RecordedBoundary | null;
   read: LedgerParent | undefined;
   /** The parent's own step in this run, when it was refreshed incrementally. */
   step: ParentStep | undefined;
}): UpstreamBounds | undefined {
   const { lineage, recorded, read, step } = args;
   const parent = lineage.parent;
   if (!parent) return undefined;
   const endAtMost = parent.builtThisRun
      ? undefined
      : parentCoverage(parent.entry, lineage.watermarkType);
   const bounds: UpstreamBounds = endAtMost ? { endAtMost } : {};
   // A full parent's table is a fresh build from raw each time: a delta over
   // it with the extension's own lower bound is its contract over raw.
   if (!parent.parentIncremental) return bounds;
   // No boundary: the extension seeds anyway, with its own reason.
   if (!recorded) return bounds;
   const judged = judgeGeneration(parent, read, step);
   if (parent.coordinate === "reseed_on_parent_change") {
      if (!judged.proven || judged.changed) {
         return {
            reseed: {
               reasonCode: "upstream_watermark_differs",
               reason:
                  `its watermark "${lineage.watermarkName}" is not its ` +
                  `incremental parent '${parent.name}''s, and the parent's ` +
                  `table ${judged.proven ? "changed" : "may have changed"} ` +
                  `since it was last read, so it is rebuilt over that table`,
            },
         };
      }
      return bounds;
   }
   if (!judged.proven) {
      return {
         reseed: {
            reasonCode: "upstream_generation_skipped",
            reason: read
               ? `the build of its parent '${parent.name}''s table it last ` +
                 `read is not the one the parent was refreshed from in this ` +
                 `run (or holds now), so a delta could miss rows a refresh ` +
                 `it never read restated`
               : `no record says which build of its parent '${parent.name}''s ` +
                 `table it last read (neither the run's ledger entry nor the ` +
                 `bound manifest), so a delta is not proven`,
         },
      };
   }
   return step?.mode === "delta" && step.start
      ? { ...bounds, startAtMost: step.start }
      : bounds;
}

/**
 * Whether `read` is the build of the parent's table this run's parent step
 * started from (or the parent holds now, when it was not refreshed), and
 * whether the parent's table changed in the run.
 */
function judgeGeneration(
   parent: ExtensionParent,
   read: LedgerParent | undefined,
   step: ParentStep | undefined,
): { proven: boolean; changed: boolean } {
   if (step) {
      // A parent seeded this run is a fresh build from raw: the extension
      // deltas over it as over a full parent.
      if (step.mode === "seed") return { proven: true, changed: true };
      return {
         proven:
            read !== undefined &&
            sameBuild(read, step.priorAdvancedBy, step.prior),
         changed: step.mode === "delta",
      };
   }
   // Built in full this run though declared incremental (a fallback rebuild
   // the step map did not see): a fresh build from raw, as a seed.
   if (parent.builtThisRun) return { proven: true, changed: true };
   const entry = parent.entry;
   const proven =
      read !== undefined &&
      entry !== undefined &&
      ((read.builtAt !== undefined && read.builtAt === entry.builtAt) ||
         sameBuild(read, entry.ledger?.advancedBy, ledgerBound(entry)));
   return { proven, changed: !proven };
}

/**
 * Whether `read` names the build of the parent's table that `advancedBy`
 * (the run that last changed it) and `bound` describe. By the run when both
 * record it: two refreshes in one second reach one boundary, so the
 * boundary alone cannot tell them apart. By the boundary otherwise (an
 * entry an older release reported).
 */
function sameBuild(
   read: LedgerParent,
   advancedBy: string | undefined,
   bound: WatermarkBound | undefined,
): boolean {
   if (read.advancedBy !== undefined && advancedBy !== undefined) {
      return read.advancedBy === advancedBy && sameCoverage(read, bound);
   }
   return sameCoverage(read, bound);
}

/** Whether a recorded read names exactly the parent boundary `bound`. */
function sameCoverage(
   read: LedgerParent,
   bound: WatermarkBound | undefined,
): boolean {
   if (
      bound === undefined ||
      read.coveredThrough === undefined ||
      // `builtFrom` records the value alone, in the parent's own type.
      (read.coveredThroughType !== undefined &&
         read.coveredThroughType !== bound.malloyType)
   ) {
      return false;
   }
   try {
      return (
         compareBounds(
            { malloyType: bound.malloyType, value: read.coveredThrough },
            bound,
         ) === 0
      );
   } catch {
      return false;
   }
}

/** An entry's incremental boundary, when it has one. */
function ledgerBound(entry: ManifestEntry): WatermarkBound | undefined {
   const ledger = entry.ledger;
   return ledger
      ? { malloyType: ledger.coveredThroughType, value: ledger.coveredThrough }
      : undefined;
}

/**
 * How far a carried parent's table reaches, as a bound of the extension's
 * time watermark type: its incremental boundary, else its `dataAsOf`.
 * Undefined for a number or string watermark, whose frontier is read from
 * the parent's own table, or when the entry records neither.
 */
export function parentCoverage(
   entry: ManifestEntry | undefined,
   watermarkType: string,
): WatermarkBound | undefined {
   if (watermarkType !== "date" && watermarkType !== "timestamp") {
      return undefined;
   }
   const ledger = entry?.ledger;
   const text =
      ledger &&
      (ledger.coveredThroughType === "date" ||
         ledger.coveredThroughType === "timestamp")
         ? ledger.coveredThrough
         : entry?.dataAsOf;
   if (text === undefined) return undefined;
   const millis = utcMillis(text);
   return millis === undefined
      ? undefined
      : canonicalBoundValue(watermarkType, new Date(millis).toISOString());
}

/** A UTC date or timestamp's text as epoch milliseconds. */
function utcMillis(text: string): number | undefined {
   const trimmed = text.trim();
   const iso = /^\d{4}-\d{2}-\d{2}$/.test(trimmed)
      ? `${trimmed}T00:00:00Z`
      : /(Z|[+-]\d{2}(:?\d{2})?)$/.test(trimmed)
        ? trimmed.replace(" ", "T")
        : `${trimmed.replace(" ", "T")}Z`;
   const millis = Date.parse(iso);
   return Number.isFinite(millis) ? millis : undefined;
}

/**
 * Which build of its parent's table an extension's boundary is reached over:
 * the parent's entry as the run holds it when the extension built. Absent
 * for any other source, or a parent with no entry.
 */
export function ledgerParentOf(
   lineage: Pick<IncrementalLineage, "parent"> | undefined,
): LedgerParent | undefined {
   const parent = lineage?.parent;
   if (!parent?.entry) return undefined;
   const bound = ledgerBound(parent.entry);
   return {
      sourceEntityId: parent.address,
      ...(parent.entry.ledger?.advancedBy
         ? { advancedBy: parent.entry.ledger.advancedBy }
         : {}),
      ...(parent.entry.builtAt ? { builtAt: parent.entry.builtAt } : {}),
      ...(bound
         ? {
              coveredThrough: bound.value,
              coveredThroughType: bound.malloyType,
           }
         : {}),
   };
}

/**
 * For a standalone run: which build of each parent's table each bound entry
 * was reached over, from its ledger's `parent`, else its `builtFrom`, by the
 * entry's address. What an extension last read when the local ledger store
 * (which holds no parent) cannot say.
 */
export function boundParentReads(
   entries: Iterable<[string, Pick<ManifestEntry, "ledger" | "builtFrom">]>,
): Map<string, LedgerParent[]> {
   const out = new Map<string, LedgerParent[]>();
   for (const [address, entry] of entries) {
      const reads: LedgerParent[] = [];
      if (entry.ledger?.parent) reads.push(entry.ledger.parent);
      for (const read of entry.builtFrom ?? []) {
         reads.push({
            sourceEntityId: read.sourceEntityId,
            ...(read.builtAt ? { builtAt: read.builtAt } : {}),
            ...(read.coveredThrough
               ? { coveredThrough: read.coveredThrough }
               : {}),
         });
      }
      if (reads.length > 0) out.set(address, reads);
   }
   return out;
}
