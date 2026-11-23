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
 *    one reached from exactly that build by deltas alone, in this run or in
 *    earlier ones the parent's ledger lists (`advances`), starting no later
 *    than the lowest of their starts; otherwise it rebuilds in full over the
 *    parent's table (`upstream_generation_skipped`).
 *
 * Pure.
 */

import {
   LEDGER_ADVANCES_CAP,
   type LedgerAdvanceChain,
   readAdvanceChain,
} from "../domain/incremental";
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
   /**
    * The table's history since its last full rebuild as recorded before this
    * run's step (the ledger's `seededBy` and `advances`), when it was.
    */
   priorChain?: LedgerAdvanceChain;
   /**
    * The `builtAt` of the table's entry in the bound manifest, when that
    * entry describes the build this step started from (its ledger's
    * `advancedBy` is `priorAdvancedBy`).
    */
   priorBuiltAt?: string;
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
   const judged = judgeGeneration(parent, read, step, lineage.watermarkType);
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
            reason: !read
               ? `no record says which build of its parent '${parent.name}''s ` +
                 `table it last read (neither the run's ledger entry nor the ` +
                 `bound manifest), so a delta is not proven`
               : judged.overflow
                 ? `its parent '${parent.name}' was refreshed more than ` +
                   `${LEDGER_ADVANCES_CAP} times since it last read the ` +
                   `parent's table, more than the parent's ledger lists, so ` +
                   `a delta is not proven`
                 : `the build of its parent '${parent.name}''s table it last ` +
                   `read is not one the parent's table was reached from by ` +
                   `deltas alone (the parent was rebuilt in full since, or ` +
                   `no record lists its refreshes), so a delta could miss ` +
                   `rows a refresh it never read restated`,
            ...(read && judged.overflow ? { overflow: true as const } : {}),
         },
      };
   }
   const startAtMost = earliest(
      step?.mode === "delta" ? step.start : undefined,
      judged.since,
   );
   return startAtMost ? { ...bounds, startAtMost } : bounds;
}

/** The earlier of two bounds, either absent. */
function earliest(
   a: WatermarkBound | undefined,
   b: WatermarkBound | undefined,
): WatermarkBound | undefined {
   if (!a) return b;
   if (!b) return a;
   return compareBounds(a, b) <= 0 ? a : b;
}

/**
 * Whether the extension may delta over the parent's table: `read` is the
 * build this run's parent step started from (or the parent holds now, when
 * it was not refreshed), or one that build was reached from by deltas alone
 * in earlier runs, whose lowest start is `since`. `changed`: the parent's
 * table changed since `read`. `overflow`: unproven because the parent was
 * refreshed more often than its ledger lists.
 */
function judgeGeneration(
   parent: ExtensionParent,
   read: LedgerParent | undefined,
   step: ParentStep | undefined,
   watermarkType: string,
): {
   proven: boolean;
   changed: boolean;
   since?: WatermarkBound;
   overflow?: boolean;
} {
   if (step) {
      // A parent seeded this run is a fresh build from raw: the extension
      // deltas over it as over a full parent.
      if (step.mode === "seed") return { proven: true, changed: true };
      if (
         read !== undefined &&
         (sameBuild(read, step.priorAdvancedBy, step.prior) ||
            (read.builtAt !== undefined && read.builtAt === step.priorBuiltAt))
      ) {
         return { proven: true, changed: step.mode === "delta" };
      }
      const chained = deltasSince(
         read,
         step.priorChain,
         step.priorAdvancedBy,
         watermarkType,
      );
      return chained.proven
         ? { proven: true, changed: true, since: chained.since }
         : { proven: false, changed: true, overflow: chained.overflow };
   }
   // Built in full this run though declared incremental (a fallback rebuild
   // the step map did not see): a fresh build from raw, as a seed.
   if (parent.builtThisRun) return { proven: true, changed: true };
   const entry = parent.entry;
   if (
      read !== undefined &&
      entry !== undefined &&
      ((read.builtAt !== undefined && read.builtAt === entry.builtAt) ||
         sameBuild(read, entry.ledger?.advancedBy, ledgerBound(entry)))
   ) {
      return { proven: true, changed: false };
   }
   // Refreshed by deltas in earlier runs, which its ledger lists.
   const chained = deltasSince(
      read,
      readAdvanceChain(entry?.ledger as Record<string, unknown> | undefined),
      entry?.ledger?.advancedBy,
      watermarkType,
   );
   return chained.proven
      ? { proven: true, changed: true, since: chained.since }
      : { proven: false, changed: true, overflow: chained.overflow };
}

/**
 * Whether the parent's build `current` (the run that last changed its table)
 * was reached from the build `read` names by deltas alone, as `chain` lists
 * them, and the lowest watermark those deltas restated. Unproven when any
 * record is missing, the parent was rebuilt in full since (`seededBy`
 * differs), the read build is older than the oldest listed delta (`overflow`
 * when the list is full), or a start is not in the extension's watermark
 * type.
 */
function deltasSince(
   read: LedgerParent | undefined,
   chain: LedgerAdvanceChain | undefined,
   current: string | undefined,
   watermarkType: string,
): { proven: boolean; since?: WatermarkBound; overflow?: boolean } {
   const no = { proven: false };
   if (!read?.advancedBy || !read.seededBy || !chain || !current) return no;
   if (read.seededBy !== chain.seededBy) return no;
   if (read.advancedBy === current) return no; // sameBuild judged that
   const first = chain.advances.findIndex((a) => a.from === read.advancedBy);
   if (first < 0) {
      return {
         proven: false,
         overflow: chain.advances.length >= LEDGER_ADVANCES_CAP,
      };
   }
   const later = chain.advances.slice(first);
   for (let i = 1; i < later.length; i++) {
      if (later[i].from !== later[i - 1].advancedBy) return no;
   }
   if (later[later.length - 1].advancedBy !== current) return no;
   let since: WatermarkBound | undefined;
   for (const a of later) {
      if (a.startType !== watermarkType) return no;
      try {
         since = earliest(since, { malloyType: a.startType, value: a.start });
      } catch {
         return no;
      }
   }
   return { proven: true, since };
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
   const seededBy = readAdvanceChain(
      parent.entry.ledger as Record<string, unknown> | undefined,
   )?.seededBy;
   return {
      sourceEntityId: parent.address,
      ...(parent.entry.ledger?.advancedBy
         ? { advancedBy: parent.entry.ledger.advancedBy }
         : {}),
      ...(seededBy ? { seededBy } : {}),
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
 * Each incremental entry's `builtAt` and `ledger`, by its address: which
 * build of its table the entry describes (incremental_build.ts,
 * `boundEntries`).
 */
export function incrementalEntriesOf(
   entries: Iterable<[string, Pick<ManifestEntry, "ledger" | "builtAt">]>,
): Map<string, Pick<ManifestEntry, "builtAt" | "ledger">> {
   const out = new Map<string, Pick<ManifestEntry, "builtAt" | "ledger">>();
   for (const [address, entry] of entries) {
      if (!entry.ledger) continue;
      out.set(address, {
         ledger: entry.ledger,
         ...(entry.builtAt ? { builtAt: entry.builtAt } : {}),
      });
   }
   return out;
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
