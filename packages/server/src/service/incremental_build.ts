import type {
   ModelMaterializer,
   PersistSource,
   BuildManifest,
} from "@malloydata/malloy";

import { logger } from "../logger";
import { recordIncrementalStep } from "../materialization_metrics";
import type {
   IncrementalLedgerEntry,
   IncrementalStrategy,
} from "../storage/DatabaseInterface";
import { errMessage } from "../utils";
import {
   deltaQueryText,
   INCREMENTAL_DIALECT_ALLOWLIST,
   isRenderableWatermarkType,
   planIncrementalStep,
   probePostgresVersion,
   seedCoveredThrough,
   type IncrementalLineage,
   type IncrementalStep,
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

/** The ledger surface a build needs; a narrowing of ResourceRepository. */
export interface IncrementalLedgerStore {
   getIncrementalLedgerEntry(
      environmentId: string,
      packageName: string,
      sourceEntityId: string,
   ): Promise<IncrementalLedgerEntry | null>;
   upsertIncrementalLedgerEntry(
      entry: Omit<IncrementalLedgerEntry, "createdAt" | "advancedAt">,
   ): Promise<IncrementalLedgerEntry>;
   deleteIncrementalLedgerEntry(
      environmentId: string,
      packageName: string,
      sourceEntityId: string,
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
   /** sourceID -> the materializer that compiles its delta query. */
   materializers: Record<string, ModelMaterializer>;
   ledger: IncrementalLedgerStore;
}

/**
 * Whether this run's `forceRefresh` should send an incremental source all the way
 * back to a full SEED, as opposed to merely getting it built.
 *
 * The two meanings have to be separated because `forceRefresh` on the wire says
 * only "build even though the content address is unchanged". A SCHEDULER fire
 * sets it for exactly that reason and no other — without it a schedule could
 * never pick up new source rows, since the address does not move when data does.
 * An incremental source is already exempt from skip-if-unchanged (see
 * MaterializationService.deriveSelfInstructions), so a scheduled force carries no
 * further instruction for it, and reading it as "and re-seed from scratch" would
 * make every scheduled run a full rebuild — the schedule could never drive a
 * delta, which is the one thing an incremental source is scheduled to do.
 *
 * An ON_DEMAND force is a person or a host asking, with a source in hand, for the
 * table to be rebuilt. That still means full: it is the escape hatch for a
 * boundary or a table that is no longer trusted.
 */
export function forcesFullSeed(run: {
   forceRefresh: boolean;
   trigger?: "ON_DEMAND" | "SCHEDULER";
}): boolean {
   return run.forceRefresh && run.trigger !== "SCHEDULER";
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
      watermarkName: watermark.name,
      watermarkType: watermark.malloyType,
      mergeKeys,
      strategy: d.strategy as IncrementalStrategy,
   };
}

/**
 * Compile the delta query for a range into SQL plus its output column names.
 *
 * Compiled WITH the build's manifest, which is not optional: a delta over a
 * chained source has to read its upstream's materialized table for exactly the
 * reason the seed does, and a manifest-less compile would silently recompute the
 * upstream from raw — a correct-looking delta over the wrong input.
 *
 * The column names come from the prepared result rather than from the source's
 * field list, because they are what the DML has to name, and the two can differ
 * in ORDER (the field list is declaration order, the SQL is projection order).
 */
export function compileDeltaFor(params: {
   materializer: ModelMaterializer;
   sourceName: string;
   watermarkName: string;
   buildManifest: BuildManifest;
   connectionDigests: Record<string, string>;
}): (
   start: WatermarkBound,
   end: WatermarkBound,
) => Promise<{ sql: string; columns: string[] }> {
   return async (start, end) => {
      const prepared = await params.materializer
         .loadQuery(
            deltaQueryText({
               sourceName: params.sourceName,
               watermarkName: params.watermarkName,
               start,
               end,
            }),
         )
         .getPreparedResult({
            buildManifest: params.buildManifest,
            connectionDigests: params.connectionDigests,
         });
      return {
         sql: prepared.sql,
         columns: prepared.resultExplore.allFields
            .map((f) => f.name)
            .filter((n): n is string => typeof n === "string" && n.length > 0),
      };
   };
}

/**
 * Decide this source's refresh: a delta to apply, a seed to fall back to, or
 * nothing to do.
 *
 * Degrades to a seed on any failure, including an unreadable ledger. The ledger
 * is publisher-local state ABOUT a warehouse table, so when the two cannot be
 * reconciled the table wins: rebuild it and record the boundary again.
 */
export async function planSourceRefresh(params: {
   context: IncrementalRunContext;
   lineage: IncrementalLineage;
   persistSource: PersistSource;
   sourceID: string;
   sourceEntityId: string;
   quotedTablePath: string;
   sourceSQL: string;
   runner: SqlRunner;
   buildManifest: BuildManifest;
   connectionDigests: Record<string, string>;
}): Promise<IncrementalStep> {
   const { context, lineage } = params;
   const materializer = context.materializers[params.sourceID];
   if (!materializer) {
      return {
         mode: "seed",
         reason:
            "the source's compiled model is not available to build a delta",
      };
   }
   let ledgerEntry: IncrementalLedgerEntry | null = null;
   try {
      ledgerEntry = await context.ledger.getIncrementalLedgerEntry(
         context.environmentId,
         context.packageName,
         params.sourceEntityId,
      );
   } catch (err) {
      return {
         mode: "seed",
         reason: `the covered_through ledger could not be read (${errMessage(err)})`,
      };
   }
   // Only asked when it can change the answer: the version gates MERGE alone,
   // and a source with no recorded boundary is seeding regardless.
   const postgresVersionNum =
      lineage.strategy === "merge" &&
      params.persistSource.dialectName === "postgres" &&
      ledgerEntry !== null
         ? await probePostgresVersion(params.runner)
         : undefined;
   try {
      return await planIncrementalStep({
         runner: params.runner,
         dialect: params.persistSource.dialectName,
         quotedTablePath: params.quotedTablePath,
         lineage,
         ledgerEntry,
         forceRefresh: context.forceRefresh,
         now: context.now,
         sourceSQL: params.sourceSQL,
         postgresVersionNum,
         compileDelta: compileDeltaFor({
            materializer,
            sourceName: params.persistSource.name,
            watermarkName: lineage.watermarkName,
            buildManifest: params.buildManifest,
            connectionDigests: params.connectionDigests,
         }),
      });
   } catch (err) {
      return {
         mode: "seed",
         reason: `the delta could not be planned (${errMessage(err)})`,
      };
   }
}

/**
 * Record a boundary the run reached. Best-effort, and deliberately so: the table
 * is already correct at this point, and a ledger write that fails must not fail
 * the build. The cost of losing the write is one full rebuild on the next run,
 * which is the same thing the ledger row is there to avoid — never wrong data.
 */
export async function advanceLedger(params: {
   context: IncrementalRunContext;
   lineage: IncrementalLineage;
   sourceEntityId: string;
   coveredThrough: WatermarkBound;
}): Promise<void> {
   const { context, lineage } = params;
   try {
      await context.ledger.upsertIncrementalLedgerEntry({
         environmentId: context.environmentId,
         packageName: context.packageName,
         sourceEntityId: params.sourceEntityId,
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
         sourceEntityId: params.sourceEntityId,
         error: errMessage(err),
      });
   }
}

/**
 * Drop a source's boundary, so the next refresh seeds. Called when a full
 * rebuild is about to run: between the rebuild's start and its boundary write,
 * the recorded value describes a table that no longer exists, and a crash in that
 * window must not leave a delta reading it.
 */
export async function resetLedger(
   context: IncrementalRunContext,
   sourceEntityId: string,
): Promise<void> {
   try {
      await context.ledger.deleteIncrementalLedgerEntry(
         context.environmentId,
         context.packageName,
         sourceEntityId,
      );
   } catch (err) {
      logger.warn("Failed to clear the covered_through boundary", {
         packageName: context.packageName,
         sourceEntityId,
         error: errMessage(err),
      });
   }
}

/**
 * Record the boundary a completed SEED reached, having just rebuilt the table.
 * Probes the table it wrote (see {@link seedCoveredThrough}); a source with no
 * rows to bound records nothing, leaving the next run to seed again.
 */
export async function advanceLedgerAfterSeed(params: {
   context: IncrementalRunContext;
   lineage: IncrementalLineage;
   sourceEntityId: string;
   quotedTablePath: string;
   runner: SqlRunner;
   dialect: string;
}): Promise<void> {
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
               sourceEntityId: params.sourceEntityId,
               error: boundary.error,
            },
         );
      }
      return;
   }
   await advanceLedger({
      context: params.context,
      lineage: params.lineage,
      sourceEntityId: params.sourceEntityId,
      coveredThrough: boundary.bound,
   });
}

/** Log and count a step, so a silent downgrade to a full rebuild is visible. */
export function reportIncrementalStep(params: {
   step: IncrementalStep;
   sourceName: string;
   packageName: string;
   physicalTableName: string;
}): void {
   const { step, sourceName, packageName, physicalTableName } = params;
   recordIncrementalStep(step.mode);
   if (step.mode === "delta") {
      logger.info("Applying an incremental delta", {
         packageName,
         sourceName,
         physicalTableName,
         rangeStart: step.start.value,
         rangeEnd: step.end.value,
      });
      return;
   }
   // A seed is the fallback for everything the delta path cannot prove, so its
   // reason is the only signal that a source declared incremental is quietly
   // being rebuilt in full every run. Logged at warn for that reason.
   logger.warn(
      step.mode === "seed"
         ? "Rebuilding an incremental source in full"
         : "Skipping an incremental source's refresh",
      { packageName, sourceName, physicalTableName, reason: step.reason },
   );
}
