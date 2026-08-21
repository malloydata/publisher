// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { PersistSource } from "@malloydata/malloy";

import { logger } from "../logger";
import { recordIncrementalStep } from "../materialization_metrics";
import type {
   IncrementalLedgerEntry,
   IncrementalStrategy,
   LedgerEntry,
} from "../storage/DatabaseInterface";
import { errMessage } from "../utils";
import {
   INCREMENTAL_DIALECT_ALLOWLIST,
   isRenderableWatermarkType,
   planIncrementalStep,
   probePostgresVersion,
   seedCoveredThrough,
   type IncrementalLineage,
   type IncrementalStep,
   type RecordedBoundary,
   type SqlRunner,
   type WatermarkBound,
} from "./incremental_apply";
import type { IncrementalDeclaration } from "./incremental_declaration";

/**
 * The run-level half of incremental materialization: what one build needs to
 * decide, per source, whether this refresh is a bounded DELTA or a full SEED, and
 * to record the boundary it reached.
 *
 * The mechanics — probes, range arithmetic, DML text — live in
 * incremental_apply.ts, which knows nothing about a run. This module is the seam
 * between that and the build loop: it reads the ledger, calls the planner, issues
 * the DML, and writes the boundary back. Splitting it this way keeps the planner
 * testable against fakes and keeps the build loop free of ledger bookkeeping.
 */

/**
 * The ledger surface a build needs; a narrowing of ResourceRepository.
 *
 * Keyed on (environment, connection, physical table): a boundary is a fact about a
 * TABLE, so every accessor here is addressed the way the lineage already is. The
 * three functions below therefore never need a source address to find a row —
 * they pass `lineage`, which carries both halves of the table key.
 */
export interface IncrementalLedgerStore {
   getIncrementalLedgerEntry(
      environmentId: string,
      connectionName: string,
      physicalTableName: string,
   ): Promise<IncrementalLedgerEntry | null>;
   upsertIncrementalLedgerEntry(
      entry: Omit<IncrementalLedgerEntry, "createdAt" | "advancedAt">,
   ): Promise<IncrementalLedgerEntry>;
   deleteIncrementalLedgerEntry(
      environmentId: string,
      connectionName: string,
      physicalTableName: string,
   ): Promise<void>;
}

/**
 * One build's incremental context, assembled once per run. Every field is
 * constant for the run; a source contributes only its own identity.
 *
 * `now` is the run's start time and is captured ONCE on purpose: it is the range
 * end for every time-watermarked source in the run, so taking it per source would
 * let two sources in one build disagree about when "now" was, and a downstream
 * source could then record coverage its upstream does not have.
 */
export interface IncrementalRunContext {
   environmentId: string;
   packageName: string;
   materializationId: string;
   forceRefresh: boolean;
   now: Date;
   /** sourceID -> the source's resolved declaration. */
   declarations: Record<string, IncrementalDeclaration>;
   ledger: IncrementalLedgerStore;
   /**
    * The caller-supplied ledger (`buildInstructions.ledger`), indexed by table —
    * see {@link indexCallerLedger}. When present (even empty), it REPLACES
    * {@link ledger} for the whole run: boundaries are read from here, and
    * nothing local is read or written — not on a delta, not on a seed. A row
    * left behind "as a backup" is a landmine for a later run in the other mode,
    * and the caller's copy is authoritative-by-report anyway: the manifest
    * entry's `ledger` is how the advance travels back.
    *
    * Run-level rather than per-source because a mixed run has no coherent
    * answer: a boundary read from one authority and written to another is a
    * boundary neither can be held to.
    */
   callerLedger?: ReadonlyMap<string, RecordedBoundary>;
}

/** The caller-ledger index key: a boundary is a fact about a TABLE. */
function ledgerTableKey(
   connectionName: string,
   physicalTableName: string,
): string {
   return `${connectionName}\u0000${physicalTableName}`;
}

/**
 * Index a caller-supplied ledger by the table each entry belongs to — the same
 * key the local store uses — converting each wire entry into the boundary shape
 * the planner reads. Nothing is derived: every field is one the publisher
 * reported on a manifest entry, echoed back.
 */
export function indexCallerLedger(
   entries: LedgerEntry[],
): Map<string, RecordedBoundary> {
   const index = new Map<string, RecordedBoundary>();
   for (const entry of entries) {
      index.set(ledgerTableKey(entry.connectionName, entry.physicalTableName), {
         sourceEntityId: entry.sourceEntityId,
         coveredThroughValue: entry.coveredThrough,
         coveredThroughType: entry.coveredThroughType,
         watermarkDimension: entry.watermark,
         mergeKeyDimensions: entry.mergeKeys ?? [],
         derivedStrategy: entry.strategy,
         physicalTableName: entry.physicalTableName,
         connectionName: entry.connectionName,
      });
   }
   return index;
}

/**
 * The lineage a source's declaration implies, or undefined when this source is
 * not a candidate for a delta at all.
 *
 * Undefined here means "build it the ordinary way, and do not touch the ledger" —
 * it is the answer for the overwhelming majority of sources. A source that
 * declared incremental refresh but fails any of these checks was already REJECTED
 * at publish (see incremental_policy.ts); reaching a build with one means the
 * package predates the gates, so it builds full, exactly as it did before.
 */
export function incrementalLineage(params: {
   declaration: IncrementalDeclaration | undefined;
   dialect: string;
   physicalTableName: string;
   connectionName: string;
   /** The source's content address — see IncrementalLineage.sourceEntityId. */
   sourceEntityId: string;
   isStorageBuild: boolean;
}): IncrementalLineage | undefined {
   const d = params.declaration;
   if (!d?.incremental) return undefined;
   const watermark = d.watermark;
   if (
      watermark === undefined ||
      watermark.kind !== "dimension" ||
      !d.watermarkOrderable ||
      !isRenderableWatermarkType(watermark.malloyType) ||
      d.strategy === undefined
   ) {
      return undefined;
   }
   if (!INCREMENTAL_DIALECT_ALLOWLIST.has(params.dialect)) return undefined;
   // A `storage=` build materializes into the destination's own engine through a
   // separate build session, while a delta's DML is issued on the source
   // connection. The publish gate rejects the combination; here it simply builds
   // full.
   if (params.isStorageBuild) return undefined;
   const mergeKeys = d.mergeKeys
      .filter((k) => k.kind === "dimension")
      .map((k) => k.name);
   if (mergeKeys.length !== d.mergeKeys.length) return undefined;
   return {
      physicalTableName: params.physicalTableName,
      connectionName: params.connectionName,
      sourceEntityId: params.sourceEntityId,
      watermarkName: watermark.name,
      watermarkType: watermark.malloyType,
      mergeKeys,
      strategy: d.strategy as IncrementalStrategy,
   };
}

/**
 * Decide this source's refresh: a delta to apply, a seed to fall back to, or
 * nothing to do.
 *
 * Degrades to a seed on any failure, including an unreadable ledger. The ledger
 * is publisher-local state ABOUT a warehouse table, so when the two cannot be
 * reconciled the table wins: rebuild it and record the boundary again.
 *
 * A caller-supplied boundary reaches the same planner through the same checks,
 * with ONE difference in what a failed check means: a boundary the CALLER sent
 * that no longer describes the source it names is thrown as an error rather
 * than absorbed as a seed. The caller's input was wrong, and per the API
 * contract an invalid ledger entry is rejected, not quietly rebuilt around —
 * create-time validation catches the common case (a changed content address),
 * and this catches what only the compiled declaration can (a moved
 * `watermark=`, `merge_key=`, or strategy). Facts about the TABLE (emptied,
 * unreadable, shape drift) still seed in both modes: the input was fine, the
 * world moved.
 */
export async function planSourceRefresh(params: {
   context: IncrementalRunContext;
   lineage: IncrementalLineage;
   persistSource: PersistSource;
   quotedTablePath: string;
   /**
    * The build's own SQL for this source — `PersistSource.getSQL()` resolved
    * against the build manifest, the exact string the seed's CTAS runs — which
    * the delta wraps and filters. Passing the manifest-resolved form is not
    * optional: a delta over a chained source has to read its upstream's
    * MATERIALIZED table for the same reason the seed does, and the raw form
    * would silently recompute the upstream — a correct-looking delta over the
    * wrong input.
    */
   sourceSQL: string;
   /**
    * The source's compiled output columns, which the delta's DML names. This is
    * what the seed's CTAS gives the table its columns from, so the two agree by
    * construction; order does not matter, because the DML names the same list on
    * both sides of the INSERT (see deltaStatements).
    */
   columns: string[];
   /**
    * This source's `BuildInstruction.reseed`: the per-source ask for a full
    * rebuild, OR-ed with the run-level `reseed` the context carries. Per source
    * because one source can rebuild while the rest advance by delta in the same
    * run.
    */
   reseed?: boolean;
   runner: SqlRunner;
}): Promise<IncrementalStep> {
   const { context, lineage } = params;
   const forceRefresh = context.forceRefresh || params.reseed === true;
   const dialect = params.persistSource.dialectName;

   let ledgerEntry: RecordedBoundary | null = null;
   if (context.callerLedger) {
      // The local store is not consulted, not even as a fallback for a table the
      // caller sent no entry for. A worker's own row may be stale by a whole
      // generation (it is why this mode exists), so falling back to it would
      // reintroduce exactly the boundary the caller was asked to own. No entry
      // means the planner's ordinary `no_boundary` seed.
      ledgerEntry =
         context.callerLedger.get(
            ledgerTableKey(lineage.connectionName, lineage.physicalTableName),
         ) ?? null;
   } else {
      try {
         ledgerEntry = await context.ledger.getIncrementalLedgerEntry(
            context.environmentId,
            lineage.connectionName,
            lineage.physicalTableName,
         );
      } catch (err) {
         return {
            mode: "seed",
            reasonCode: "ledger_unreadable",
            reason: `the covered_through ledger could not be read (${errMessage(err)})`,
         };
      }
   }
   // Only asked when it can change the answer: the version gates MERGE alone,
   // and a source with no recorded boundary is seeding regardless.
   const postgresVersionNum =
      lineage.strategy === "merge" &&
      dialect === "postgres" &&
      ledgerEntry !== null
         ? await probePostgresVersion(params.runner)
         : undefined;
   let step: IncrementalStep;
   try {
      step = await planIncrementalStep({
         runner: params.runner,
         dialect,
         quotedTablePath: params.quotedTablePath,
         lineage,
         ledgerEntry,
         forceRefresh,
         now: context.now,
         sourceSQL: params.sourceSQL,
         columns: params.columns,
         postgresVersionNum,
      });
   } catch (err) {
      return {
         mode: "seed",
         reasonCode: "plan_error",
         reason: `the delta could not be planned (${errMessage(err)})`,
      };
   }
   // A caller-supplied entry that does not describe the source it names is the
   // CALLER's error, and the API contract makes it one: fail the run rather
   // than absorb it as a seed. Reachable only for a mismatch create-time
   // validation cannot see (a declaration moved without moving the content
   // address); the address itself was already checked against the plan, and the
   // table key matches by construction of the index.
   if (
      context.callerLedger &&
      step.mode === "seed" &&
      (step.reasonCode === "lineage_changed" ||
         step.reasonCode === "table_renamed")
   ) {
      throw new Error(
         `The supplied ledger entry for table "${lineage.physicalTableName}" ` +
            `was measured under a different definition of this source: ` +
            `${step.reason}. Delete the entry to rebuild the source, or set ` +
            `reseed.`,
      );
   }
   return step;
}

/**
 * Record a boundary the run reached. Best-effort, and deliberately so: the table
 * is already correct at this point, and a ledger write that fails must not fail
 * the build. The cost of losing the write is one full rebuild on the next run,
 * which is the same thing the ledger row is there to avoid — never wrong data.
 *
 * A no-op when the caller supplied the ledger: there the advance travels on the
 * manifest entry (which is written either way, from the value this would have
 * stored), and the local store is left untouched so it cannot later contradict
 * the caller.
 */
export async function advanceLedger(params: {
   context: IncrementalRunContext;
   lineage: IncrementalLineage;
   coveredThrough: WatermarkBound;
}): Promise<void> {
   const { context, lineage } = params;
   if (context.callerLedger) return;
   try {
      await context.ledger.upsertIncrementalLedgerEntry({
         environmentId: context.environmentId,
         packageName: context.packageName,
         sourceEntityId: lineage.sourceEntityId,
         coveredThroughValue: params.coveredThrough.value,
         coveredThroughType: params.coveredThrough.malloyType,
         watermarkDimension: lineage.watermarkName,
         mergeKeyDimensions: lineage.mergeKeys,
         derivedStrategy: lineage.strategy,
         physicalTableName: lineage.physicalTableName,
         connectionName: lineage.connectionName,
         advancedByMaterializationId: context.materializationId,
      });
   } catch (err) {
      logger.warn("Failed to advance the covered_through boundary", {
         packageName: context.packageName,
         physicalTableName: lineage.physicalTableName,
         sourceEntityId: lineage.sourceEntityId,
         error: errMessage(err),
      });
   }
}

/**
 * Drop a source's boundary, so the next refresh seeds. Called when a full
 * rebuild is about to run: between the rebuild's start and its boundary write,
 * the recorded value describes a table that no longer exists, and a crash in that
 * window must not leave a delta reading it.
 *
 * A no-op when the caller supplied the ledger: there is no local row to clear,
 * and the window it protects is closed differently — a run that crashes
 * mid-rebuild reports no manifest, so the caller keeps the entry it already
 * had, measured against a table the failed rebuild replaced. The next run's
 * planner sees that table's emptiness or the entry's own idempotent range,
 * never a silently skipped row (the boundary can only LAG a table the rebuild
 * was refreshing).
 */
export async function resetLedger(
   context: IncrementalRunContext,
   lineage: IncrementalLineage,
): Promise<void> {
   if (context.callerLedger) return;
   try {
      await context.ledger.deleteIncrementalLedgerEntry(
         context.environmentId,
         lineage.connectionName,
         lineage.physicalTableName,
      );
   } catch (err) {
      logger.warn("Failed to clear the covered_through boundary", {
         packageName: context.packageName,
         physicalTableName: lineage.physicalTableName,
         error: errMessage(err),
      });
   }
}

/**
 * Record the boundary a completed SEED reached, having just rebuilt the table.
 * Probes the table it wrote (see {@link seedCoveredThrough}); a source with no
 * rows to bound records nothing, leaving the next run to seed again.
 *
 * Returns the boundary it recorded (undefined when there was none to record) so
 * the caller can report it on the manifest entry. When the caller supplied the
 * ledger, the probe and the return value are the whole job — the store write
 * below is skipped — because the entry's `ledger` is the only place that
 * boundary needs to reach.
 */
export async function advanceLedgerAfterSeed(params: {
   context: IncrementalRunContext;
   lineage: IncrementalLineage;
   quotedTablePath: string;
   runner: SqlRunner;
   dialect: string;
}): Promise<WatermarkBound | undefined> {
   const boundary = await seedCoveredThrough(
      params.runner,
      params.dialect,
      params.lineage,
      params.quotedTablePath,
      params.context.now,
   );
   if (!boundary.bound) {
      if (boundary.error) {
         logger.warn(
            "Could not read a covered_through boundary after a full rebuild; " +
               "the next refresh will rebuild again",
            {
               packageName: params.context.packageName,
               physicalTableName: params.lineage.physicalTableName,
               error: boundary.error,
            },
         );
      }
      return undefined;
   }
   await advanceLedger({
      context: params.context,
      lineage: params.lineage,
      coveredThrough: boundary.bound,
   });
   return boundary.bound;
}

/**
 * Log and count a SEED or a SKIP, so a silent downgrade to a full rebuild is
 * visible. Both are terminal decisions, so reporting them where they are decided
 * is reporting what happened.
 *
 * A delta is NOT reported here, because at this point it has only been planned.
 * Its metric and its one log line are emitted on COMPLETION, with the duration
 * (see {@link reportDeltaApplied}): a planned delta that then fails would
 * otherwise be counted as work the table received, and the delta:seed ratio the
 * counter exists for would read high exactly when the feature is failing.
 */
export function reportIncrementalStep(params: {
   step: Extract<IncrementalStep, { mode: "seed" | "skip" }>;
   sourceName: string;
   packageName: string;
   physicalTableName: string;
}): void {
   const { step, sourceName, packageName, physicalTableName } = params;
   recordIncrementalStep(step.mode, step.reasonCode);
   // A seed is the fallback for everything the delta path cannot prove, so its
   // reason is the only signal that a source declared incremental is quietly
   // being rebuilt in full every run. Logged at warn for that reason.
   logger.warn(
      step.mode === "seed"
         ? "Rebuilding an incremental source in full"
         : "Skipping an incremental source's refresh",
      {
         packageName,
         sourceName,
         physicalTableName,
         reasonCode: step.reasonCode,
         reason: step.reason,
      },
   );
}

/**
 * Count and log a delta that COMMITTED. Paired with {@link reportIncrementalStep}
 * so every step is reported exactly once, and a `step="delta"` count always means
 * a table that actually received one.
 */
export function reportDeltaApplied(params: {
   sourceName: string;
   packageName: string;
   physicalTableName: string;
   rangeStart: string;
   rangeEnd: string;
   durationMs: number;
}): void {
   recordIncrementalStep("delta");
   logger.info("Applied an incremental delta", params);
}
