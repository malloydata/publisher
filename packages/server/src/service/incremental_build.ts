// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { logger } from "../logger";
import type { PersistSource } from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import {
   recordIncrementalStep,
   recordSourceBuildDuration,
} from "../materialization_metrics";
import type {
   BuildInstruction,
   IncrementalLedgerEntry,
   IncrementalStrategy,
   LedgerEntry,
   LedgerTableIdentity,
   ManifestEntry,
} from "../storage/DatabaseInterface";
import {
   collectIncrementalDeclarations,
   CompiledBuildPlan,
   deriveColumns,
} from "./build_plan";
import { storageDeltaTarget } from "./incremental_storage";
import {
   STORAGE_TARGET_DIALECT,
   type StorageIncrementalRefresh,
} from "./materialization_build_session";
import type { QueryMetadata } from "./query_metadata";
import { errMessage } from "../utils";
import {
   applyDeltaScript,
   INCREMENTAL_SOURCE_RANGE_DIALECTS,
   INCREMENTAL_TARGET_DML_DIALECTS,
   isRenderableWatermarkType,
   planIncrementalStep,
   probePostgresVersion,
   seedCoveredThrough,
   type DeltaTarget,
   type IncrementalLineage,
   type IncrementalStep,
   type RecordedBoundary,
   type SqlRunner,
   type WatermarkBound,
} from "./incremental_apply";
import type { IncrementalDeclaration } from "./incremental_declaration";

/**
 * The run-level half of incremental materialization: per source, decide whether
 * this refresh is a bounded DELTA or a full SEED, and record the boundary reached.
 *
 * The mechanics (probes, range arithmetic, DML text) live in incremental_apply.ts,
 * which knows nothing about a run. This module reads the ledger, calls the
 * planner, issues the DML and writes the boundary back, keeping the planner
 * testable against fakes and the build loop free of ledger bookkeeping.
 */

/**
 * The ledger surface a build needs; a narrowing of ResourceRepository. Addressed
 * by TABLE, because that is what a boundary is a fact about.
 */
interface IncrementalLedgerStore {
   getIncrementalLedgerEntry(
      environmentId: string,
      table: LedgerTableIdentity,
   ): Promise<IncrementalLedgerEntry | null>;
   upsertIncrementalLedgerEntry(
      entry: Omit<IncrementalLedgerEntry, "createdAt" | "advancedAt">,
   ): Promise<IncrementalLedgerEntry>;
   deleteIncrementalLedgerEntry(
      environmentId: string,
      table: LedgerTableIdentity,
   ): Promise<void>;
}

/**
 * One build's incremental context, assembled once per run; every field is
 * constant for the run.
 *
 * `now` is captured ONCE: it is the range end for every time-watermarked source,
 * so taking it per source would let a downstream source record coverage its
 * upstream does not have.
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
    * The caller-supplied ledger (`buildInstructions.ledger`), indexed by table
    * (see {@link indexCallerLedger}). When present (even empty) it REPLACES
    * {@link ledger} for the whole run: nothing local is read or written, since a
    * local "backup" row would mislead a later run in the other mode. The
    * manifest entry's `ledger` is how the advance travels back.
    *
    * Run-level because a boundary read from one authority and written to another
    * is one neither can be held to.
    */
   callerLedger?: ReadonlyMap<string, RecordedBoundary>;
}

/**
 * The caller-ledger index key: a boundary is a fact about a TABLE. Connection and
 * destination are separate NUL-delimited fields because they are separate
 * namespaces: a connection and a destination both called `lake` are different
 * warehouses.
 */
function ledgerTableKey(table: LedgerTableIdentity): string {
   return [
      table.connectionName,
      table.storageDestinationName ?? "",
      table.physicalTableName,
   ].join("\u0000");
}

/**
 * Index a caller-supplied ledger by table (the same key the local store uses),
 * converting each wire entry into the boundary shape the planner reads. Every
 * field is one the publisher reported on a manifest entry, echoed back.
 */
export function indexCallerLedger(
   entries: LedgerEntry[],
): Map<string, RecordedBoundary> {
   const index = new Map<string, RecordedBoundary>();
   for (const entry of entries) {
      index.set(
         ledgerTableKey({
            connectionName: entry.connectionName,
            storageDestinationName: entry.storageDestinationName,
            physicalTableName: entry.physicalTableName,
         }),
         {
            sourceEntityId: entry.sourceEntityId,
            coveredThroughValue: entry.coveredThrough,
            coveredThroughType: entry.coveredThroughType,
            watermarkDimension: entry.watermark,
            mergeKeyDimensions: entry.mergeKeys ?? [],
            derivedStrategy: entry.strategy,
            physicalTableName: entry.physicalTableName,
            connectionName: entry.connectionName,
            storageDestinationName: entry.storageDestinationName,
         },
      );
   }
   return index;
}

/**
 * The lineage a source's declaration implies, or undefined when this source is
 * not a candidate for a delta: build it the ordinary way and leave the ledger
 * alone. A declared-incremental source failing these checks is normally REJECTED
 * at publish (see incremental_policy.ts); one that reaches a build anyway builds
 * full.
 */
export function incrementalLineage(params: {
   declaration: IncrementalDeclaration | undefined;
   /** The SOURCE's dialect, which has to be able to express a bounded range. */
   dialect: string;
   /**
    * The dialect the DML will be issued in — the source's own for a colocated
    * table, `duckdb` for one in a `storage=` destination.
    */
   targetDialect: string;
   physicalTableName: string;
   connectionName: string;
   /**
    * The storage destination the table lives in, absent when colocated. Part of
    * the boundary's table identity, so a source that moves between destinations
    * cannot read a boundary measured on the table it left behind.
    */
   storageDestinationName?: string;
   /** The source's content address — see IncrementalLineage.sourceEntityId. */
   sourceEntityId: string;
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
   if (!INCREMENTAL_SOURCE_RANGE_DIALECTS.has(params.dialect)) return undefined;
   // The DML engine must be one whose transactional apply is proven. A legal
   // `storage=` destination is always DuckDB-family; this states the invariant
   // where it is relied on.
   if (!INCREMENTAL_TARGET_DML_DIALECTS.has(params.targetDialect)) {
      return undefined;
   }
   const mergeKeys = d.mergeKeys
      .filter((k) => k.kind === "dimension")
      .map((k) => k.name);
   if (mergeKeys.length !== d.mergeKeys.length) return undefined;
   // Backstop behind the publish-time refusal: a merge whose scope could not be
   // resolved must never be ISSUED. No lineage means a seed: slower, but correct.
   if (mergeKeys.length > 0 && d.scopeIncomplete) return undefined;
   return {
      physicalTableName: params.physicalTableName,
      connectionName: params.connectionName,
      storageDestinationName: params.storageDestinationName,
      sourceEntityId: params.sourceEntityId,
      watermarkName: watermark.name,
      watermarkType: watermark.malloyType,
      mergeKeys,
      scopeColumns: d.scopeColumns,
      strategy: d.strategy as IncrementalStrategy,
   };
}

/**
 * Decide this source's refresh: a delta to apply, a seed to fall back to, or
 * nothing to do.
 *
 * Degrades to a seed on any failure, including an unreadable ledger: the ledger
 * is local state ABOUT a warehouse table, so when they disagree the table wins.
 *
 * One exception: a CALLER-supplied boundary that no longer describes the source
 * it names (a moved `watermark=`, `merge_key=` or strategy, which create-time
 * validation cannot see) throws, because the API contract rejects an invalid
 * ledger entry rather than rebuilding around it. Facts about the TABLE (emptied,
 * unreadable, shape drift) seed in both modes.
 */
export async function planSourceRefresh(params: {
   context: IncrementalRunContext;
   lineage: IncrementalLineage;
   /**
    * Where the DML lands and how its rows are produced:
    * {@link warehouseDeltaTarget} for a colocated source, the storage target for
    * a `storage=` one. Its build SQL must be the MANIFEST-RESOLVED form, so a
    * chained source reads its upstream's materialized table; the raw form would
    * silently recompute the upstream.
    */
   target: DeltaTarget;
   /**
    * The source's compiled output columns, which the delta's DML names; the
    * seed's CTAS takes its columns from the same place. Order does not matter:
    * the DML names the same list on both sides of the INSERT (see deltaStatements).
    */
   columns: string[];
   /**
    * This source's `BuildInstruction.reseed`, OR-ed with the run-level
    * `forceRefresh`, so one source can rebuild while the rest advance by delta.
    */
   reseed?: boolean;
}): Promise<IncrementalStep> {
   const { context, lineage, target } = params;
   const forceRefresh = context.forceRefresh || params.reseed === true;

   let ledgerEntry: RecordedBoundary | null = null;
   if (context.callerLedger) {
      // No fallback to the local store for a table the caller omitted: a
      // worker's own row may be a generation stale, which is why this mode
      // exists. No entry means the ordinary `no_boundary` seed.
      ledgerEntry = context.callerLedger.get(ledgerTableKey(lineage)) ?? null;
   } else {
      try {
         ledgerEntry = await context.ledger.getIncrementalLedgerEntry(
            context.environmentId,
            lineage,
         );
      } catch (err) {
         return {
            mode: "seed",
            reasonCode: "ledger_unreadable",
            reason: `the covered_through ledger could not be read (${errMessage(err)})`,
         };
      }
   }
   // Keyed on the TARGET dialect, because the version floor is about where the
   // MERGE runs (a `storage=` source reads Postgres but writes DuckDB). Probed
   // only when it can matter: the version gates MERGE alone, and with no
   // recorded boundary the source seeds regardless.
   const postgresVersionNum =
      lineage.strategy === "merge" &&
      target.dialect === "postgres" &&
      ledgerEntry !== null
         ? await probePostgresVersion(target.runner)
         : undefined;
   let step: IncrementalStep;
   try {
      step = await planIncrementalStep({
         target,
         lineage,
         ledgerEntry,
         forceRefresh,
         now: context.now,
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
   // A caller-supplied entry that does not describe its source is the caller's
   // error: fail the run rather than seed. Reachable only when a declaration
   // moved without moving the content address, which create-time validation
   // already checked.
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
 * Record a boundary the run reached. Best-effort: the table is already correct,
 * and a lost write costs one full rebuild next run, never wrong data.
 *
 * A no-op when the caller supplied the ledger: the advance travels on the
 * manifest entry, and the local store stays untouched so it cannot later
 * contradict the caller.
 */
async function advanceLedger(params: {
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
         storageDestinationName: lineage.storageDestinationName,
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
 * Drop a source's boundary, so the next refresh seeds. Called before a full
 * rebuild: until the rebuild records a new boundary, the old one describes a
 * table that no longer exists, and a crash in that window must not leave a delta
 * reading it.
 *
 * A no-op when the caller supplied the ledger: a run that crashes mid-rebuild
 * reports no manifest, so the caller keeps its old entry, which can only LAG the
 * table, never skip a row.
 */
export async function resetLedger(
   context: IncrementalRunContext,
   lineage: IncrementalLineage,
): Promise<void> {
   if (context.callerLedger) return;
   try {
      await context.ledger.deleteIncrementalLedgerEntry(
         context.environmentId,
         lineage,
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
 * Record the boundary a completed SEED reached by probing the table it wrote
 * (see {@link seedCoveredThrough}); a source with no rows records nothing, so the
 * next run seeds again.
 *
 * Returns the boundary (undefined when none) for the manifest entry. With a
 * caller-supplied ledger the store write is skipped and the return value is the
 * whole job.
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
 * Apply the step a plan decided on and report what happened, for both colocated
 * and `storage=` tables.
 *
 * Shared because the ORDER is load-bearing: the DML commits, and only then is the
 * boundary advanced. A boundary written first would claim coverage a failed DML
 * never delivered.
 *
 * A SEED is not applied here: it returns `applied: false` and the caller runs its
 * own full rebuild.
 *
 * @returns for the manifest entry: the apply duration (absent for a skip), the
 *   boundary now in force, and what the refresh DID.
 */
export async function applyIncrementalStep(params: {
   context: IncrementalRunContext;
   lineage: IncrementalLineage;
   step: IncrementalStep;
   target: DeltaTarget;
   sourceName: string;
}): Promise<{
   applied: boolean;
   durationMs?: number;
   coveredThrough?: WatermarkBound;
   refresh?: "delta" | "none";
}> {
   const { context, lineage, step, target } = params;
   if (step.mode !== "delta") {
      reportIncrementalStep({
         step,
         sourceName: params.sourceName,
         packageName: context.packageName,
         physicalTableName: lineage.physicalTableName,
      });
   }
   if (step.mode === "seed") return { applied: false };
   if (step.mode === "skip") {
      // No duration (reported as null, not 0, which would skew the
      // build-duration series).
      return {
         applied: true,
         coveredThrough: step.coveredThrough,
         refresh: "none",
      };
   }

   const startTime = performance.now();
   // One call, so the range replace's DELETE and INSERT commit or roll back
   // together (see deltaScript). A failure leaves the table and the boundary
   // as they were; the next run recomputes the same range.
   await applyDeltaScript(target.runner, target.dialect, step.statements);
   await advanceLedger({
      context,
      lineage,
      coveredThrough: step.coveredThrough,
   });
   const durationMs = Math.round(performance.now() - startTime);
   reportDeltaApplied({
      packageName: context.packageName,
      sourceName: params.sourceName,
      physicalTableName: lineage.physicalTableName,
      rangeStart: step.start.value,
      rangeEnd: step.end.value,
      durationMs,
   });
   return {
      applied: true,
      durationMs,
      coveredThrough: step.coveredThrough,
      refresh: "delta",
   };
}

/**
 * Log and count a SEED or a SKIP, so a silent downgrade to a full rebuild is
 * visible.
 *
 * A delta is reported on COMPLETION instead (see {@link reportDeltaApplied}), so
 * a planned delta that fails is not counted and the delta:seed ratio does not
 * read high exactly when the feature is failing.
 */
export function reportIncrementalStep(params: {
   step: Extract<IncrementalStep, { mode: "seed" | "skip" }>;
   sourceName: string;
   packageName: string;
   physicalTableName: string;
}): void {
   const { step, sourceName, packageName, physicalTableName } = params;
   recordIncrementalStep(step.mode, step.reasonCode);
   // Warn: a seed's reason is the only signal that an incremental source is
   // quietly being rebuilt in full every run.
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
 * Count and log a delta that COMMITTED. With {@link reportIncrementalStep}, every
 * step is reported exactly once.
 */
function reportDeltaApplied(params: {
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

/**
 * What an incremental refresh DID to the table, as the manifest entry's field, or
 * nothing for a source not refreshed incrementally.
 *
 * Reported because it cannot be inferred: every delta-path fallback succeeds by
 * rebuilding. `none` names the do-nothing case so that absence has ONE meaning
 * (not refreshed incrementally); `ledger` cannot disambiguate, since an
 * incremental source over an empty table reports no boundary either.
 */
export function refreshFields(
   did: "delta" | "full" | "none" | undefined,
): { refresh: "delta" | "full" | "none" } | Record<never, never> {
   return did ? { refresh: did } : {};
}

/**
 * The manifest entry's `ledger` field: the boundary this refresh left in force,
 * self-contained (table, value, and the source definition it was measured under).
 * A caller that owns the ledger stores it and returns it verbatim in the next
 * run's `buildInstructions.ledger`.
 *
 * The ONLY place a boundary is reported, so it is emitted whoever owns the ledger
 * and on every path that leaves one in force (delta, seed, or skip).
 *
 * Empty when there is no boundary (a non-incremental or empty source), so the
 * spread adds no key rather than an explicit undefined.
 */
export function ledgerFields(
   lineage: IncrementalLineage | undefined,
   bound: WatermarkBound | undefined,
): { ledger: LedgerEntry } | Record<never, never> {
   if (!lineage || !bound) return {};
   return {
      ledger: {
         connectionName: lineage.connectionName,
         physicalTableName: lineage.physicalTableName,
         // Absent (not undefined) for a colocated table: absence MEANS colocated
         // on the wire.
         ...(lineage.storageDestinationName
            ? { storageDestinationName: lineage.storageDestinationName }
            : {}),
         coveredThrough: bound.value,
         coveredThroughType: bound.malloyType,
         watermark: lineage.watermarkName,
         ...(lineage.mergeKeys.length > 0
            ? { mergeKeys: lineage.mergeKeys }
            : {}),
         strategy: lineage.strategy,
         sourceEntityId: lineage.sourceEntityId,
      },
   };
}

/**
 * The run's incremental context, or undefined when no source in the package
 * asked for an incremental refresh (the common case), leaving the build path
 * untouched.
 *
 * Resolved from the compiled plan because the declaration is read off the
 * compiled source's schema and query, which the wire plan does not carry.
 */
export function incrementalRunContext(
   compiled: CompiledBuildPlan,
   run: {
      environmentId: string;
      packageName: string;
      materializationId: string;
      forceRefresh: boolean;
      now: Date;
      /**
       * The caller-supplied ledger. Undefined means the local store; present
       * (even empty) replaces it for the whole run.
       */
      ledger: LedgerEntry[] | undefined;
   },
   localLedger: IncrementalLedgerStore,
): IncrementalRunContext | undefined {
   // collectIncrementalDeclarations drops (and warns on) an unreadable
   // declaration, so that source builds full.
   const declarations: Record<string, IncrementalDeclaration> = {};
   for (const [sourceID, declaration] of Object.entries(
      collectIncrementalDeclarations(compiled.sources),
   )) {
      if (declaration.incremental) declarations[sourceID] = declaration;
   }
   if (Object.keys(declarations).length === 0) return undefined;
   const { ledger, ...rest } = run;
   return {
      ...rest,
      declarations,
      ledger: localLedger,
      ...(ledger !== undefined
         ? { callerLedger: indexCallerLedger(ledger) }
         : {}),
   };
}

/**
 * Advance one source's serving table by a bounded delta, if that is what this
 * refresh should do.
 *
 * Returns the manifest entry when the table was advanced in place (or needed
 * nothing), and `undefined` to mean "rebuild it" via the caller's ordinary CTAS;
 * {@link reportIncrementalStep} logs and counts every such fallback.
 *
 * The DML runs on the SOURCE connection with the build's run options, so the
 * warehouse attributes a delta to the same unit of work as the seed.
 */
export async function refreshOneSourceIncrementally(params: {
   context: IncrementalRunContext;
   lineage: IncrementalLineage;
   persistSource: PersistSource;
   instruction: BuildInstruction;
   target: DeltaTarget;
   manifest: Manifest;
   /**
    * How the source read its stored upstreams (see
    * {@link upstreamReuseFromManifest}). Carried on every entry, since an
    * absent field reads as "no persisted upstream".
    */
   upstreamReuse: Pick<
      ManifestEntry,
      "upstreamReuse" | "upstreamRecomputeReason"
   >;
}): Promise<ManifestEntry | undefined> {
   const { context, lineage, persistSource, instruction, target } = params;
   const sourceEntityId = instruction.sourceEntityId;

   const step = await planSourceRefresh({
      context,
      lineage,
      target,
      columns: deriveColumns(persistSource).map((c) => String(c.name)),
      reseed: instruction.reseed,
   });
   const outcome = await applyIncrementalStep({
      context,
      lineage,
      step,
      target,
      sourceName: persistSource.name,
   });
   if (!outcome.applied) return undefined;
   if (outcome.durationMs !== undefined) {
      recordSourceBuildDuration(outcome.durationMs, "delta");
   }

   // A delta or skip leaves the existing table serving; it must still enter the
   // build manifest, or a downstream source in this run would recompute it from
   // raw.
   params.manifest.update(sourceEntityId, {
      tableName: target.quotedTablePath,
   });
   return {
      sourceEntityId,
      sourceName: persistSource.name,
      materializedTableId: instruction.materializedTableId,
      physicalTableName: lineage.physicalTableName,
      connectionName: persistSource.connectionName,
      realization: instruction.realization,
      ...params.upstreamReuse,
      rowCount: null,
      buildDurationMs: outcome.durationMs ?? null,
      // applyDeltaScript returns no cost figure, even on backends that have one.
      queryCostBytes: null,
      // A delta reports where it advanced to; a skip, the boundary in force.
      ...ledgerFields(lineage, outcome.coveredThrough),
      ...refreshFields(outcome.refresh),
   };
}

/**
 * The in-place refresh a stored source may get instead of a rebuild, as two hooks
 * in the build session: before the CTAS (advance, or decline and let the rebuild
 * run) and after it (record where the rebuilt table reaches).
 *
 * Callbacks because both probe the destination, and the read-write session on it
 * exists only for this one source's refresh.
 */
export function storageRefreshFor(params: {
   context: IncrementalRunContext;
   lineage: IncrementalLineage;
   persistSource: PersistSource;
   instruction: BuildInstruction;
   destinationName: string;
   /** The destination's schema (see storageTablePath); absent is `main`. */
   destinationSchema?: string;
   physicalTableName: string;
   /** The unprojected build SQL — see storageDeltaTarget. */
   buildSQL: string;
   queryMetadata?: QueryMetadata;
}): StorageIncrementalRefresh {
   const { context, lineage, persistSource, instruction } = params;
   return {
      plan: async ({ session, sourceType, handle, quotedTablePath }) => {
         const { target, readCost } = storageDeltaTarget({
            session,
            sourceType,
            handle,
            destinationName: params.destinationName,
            destinationSchema: params.destinationSchema,
            physicalTableName: params.physicalTableName,
            quotedTablePath,
            lineage,
            persistSource,
            buildSQL: params.buildSQL,
            queryMetadata: params.queryMetadata,
         });
         const step = await planSourceRefresh({
            context,
            lineage,
            target,
            columns: deriveColumns(persistSource).map((c) => String(c.name)),
            reseed: instruction.reseed,
         });
         const outcome = await applyIncrementalStep({
            context,
            lineage,
            step,
            target,
            sourceName: persistSource.name,
         });
         if (!outcome.applied) {
            // Drop the boundary BEFORE the rebuild, not after: a crash
            // mid-rebuild must not leave a boundary for data that no longer
            // exists.
            await resetLedger(context, lineage);
            return undefined;
         }
         return {
            durationMs: outcome.durationMs,
            coveredThrough: outcome.coveredThrough,
            refresh: outcome.refresh ?? "none",
            readCost: readCost(),
         };
      },
      afterSeed: ({ session, quotedTablePath }) =>
         advanceLedgerAfterSeed({
            context,
            lineage,
            quotedTablePath,
            dialect: STORAGE_TARGET_DIALECT,
            runner: (sql) => session.runSQL(sql),
         }),
   };
}
