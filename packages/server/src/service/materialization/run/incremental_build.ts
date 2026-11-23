// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The run-level half of incremental materialization: per source, decide whether
 * this refresh is a bounded DELTA or a full SEED, and record the boundary reached.
 *
 * The mechanics (probes, range arithmetic, DML text) live in incremental_apply.ts,
 * which knows nothing about a run. This module reads the ledger, calls the
 * planner, issues the DML and writes the boundary back, keeping the planner
 * testable against fakes and the build loop free of ledger bookkeeping.
 *
 * Surface: the run's context ({@link incrementalRunContext}), a source's
 * refresh and lineage ({@link sourceIncrementalRefresh},
 * {@link incrementalLineage}), the plan and its step
 * ({@link planSourceRefresh}, {@link applyIncrementalStep},
 * {@link refreshOneSourceIncrementally}, {@link storageRefreshFor}), and the
 * ledger writes ({@link resetLedger}, {@link advanceLedgerAfterSeed}). Read by
 * the two tier strategies, the instructed build and the service.
 *
 * Effects: the delta's SQL in the warehouse or destination, the incremental
 * ledger in the run store; logs and counts each step.
 */

import { captureDatabaseContract } from "./column_contract_capture";
import type { Connection as ReadConnectionSource } from "@malloydata/malloy";
import { errorCodeOf } from "../../../error_code";
import { recordedTableKey } from "../domain/physical_table";
import { logger } from "../../../logger";
import {
   persistSourceDeclaredColumns,
   persistSourceQueryTimezone,
   rebuildWouldRecast,
   STORED_COLUMN_TYPE_MISMATCH_CODE,
   type StoredColumnTypeMismatch,
} from "../stored_column_types";
import type { PersistSource } from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import { recordIncrementalStep } from "../../../materialization_metrics";
import type {
   IncrementalLedgerEntry,
   LedgerTableIdentity,
} from "../../../storage/DatabaseInterface";
import type {
   BuildInstruction,
   IncrementalStrategy,
   LateRows,
   LedgerEntry,
   MalformedLedgerEntry,
   ManifestEntry,
} from "../domain/types";
import {
   collectIncrementalDeclarations,
   deriveColumns,
   type RunPlan,
   storedColumnOf,
} from "./run_plan";
import { capabilitiesOf, STORAGE_DIALECT } from "../domain/dialect";
import { stackedDeltaTarget, storageDeltaTarget } from "./incremental_storage";
import {
   type StackedIncrementalRefresh,
   type StorageIncrementalRefresh,
} from "./materialization_build_session";
import type { QueryMetadata } from "../../query_metadata";
import { errMessage } from "../../../utils";
import {
   applyDeltaScript,
   countLateRows,
   isPairGuardFailure,
   isRenderableWatermarkType,
   planIncrementalStep,
   probeServerVersion,
   seedCoveredThrough,
   type DeltaTarget,
   type ExtensionParent,
   type IncrementalLineage,
   type LedgerParent,
   type IncrementalStep,
   type RecordedBoundary,
   type SqlRunner,
   type WatermarkBound,
} from "./incremental_apply";
import { isDecimalDeltaGuardFailure } from "./passthrough/postgres";
import { capturedNativeSchema } from "./database_native_types";
import {
   coordinateWithParent,
   ledgerParentOf,
   type ParentStep,
   type UnprovenGeneration,
} from "./incremental_upstream";
import {
   INCREMENTAL_LATE_ROWS_CODE,
   LEDGER_ADVANCES_CAP,
   type LedgerAdvanceChain,
   MATERIALIZE_EXTENSION_GENERATION_UNPROVEN_CODE,
   type IncrementalDeclaration,
   readAdvanceChain,
} from "../domain/incremental";

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
   /**
    * The defect of each entry the controller dropped from {@link callerLedger}
    * as malformed, by the `physicalTableName` it named. Its source seeds as
    * any source with no entry does, under `ledger_entry_malformed` rather than
    * `no_boundary`, so a corrupted entry is told apart from a missing one.
    */
   malformedCallerLedger?: ReadonlyMap<string, string>;
   /**
    * The `lateRows` the package's bound manifest reports, by content address:
    * what earlier deltas' late-row checks found and no rebuild has cleared.
    * A delta or a skip carries it onto its entry, so the warning stays until
    * the source is rebuilt in full. Read from the BOUND manifest, so a run a
    * host instructs carries what the host bound, with no field of its own.
    */
   priorLateRows?: ReadonlyMap<string, LateRows>;
   /**
    * Each incremental source's step in this run, by content address, filled
    * as it is planned: what an extension over it reads to follow it
    * ({@link coordinateWithParent}).
    */
   parentSteps?: Map<string, ParentStep>;
   /**
    * For a standalone run: which build of each parent's table each bound
    * entry was reached over, by the entry's address, from the manifest bound
    * when the run started ({@link boundParentReads}). Not read when the
    * caller supplies the ledger: a worker's bound manifest may lag the
    * host's, and the ledger entry carries it.
    */
   boundParentReads?: ReadonlyMap<string, readonly LedgerParent[]>;
   /**
    * The extensions this run rebuilt because no record said which build of
    * their parent's table they last read; the package warns about them.
    */
   unprovenGenerations?: UnprovenGeneration[];
   /**
    * For each source whose refresh applied nothing: the run that last
    * changed its table, from its recorded boundary, by content address.
    */
   skippedAdvancedBy?: Map<string, string>;
   /**
    * The package's bound manifest entries' `builtAt` and `ledger`, by content
    * address, as bound when the run started: the build an incremental
    * source's step starts from, as its entry names it, and (standalone, where
    * the local ledger store keeps no extension keys) its recorded history
    * (`seededBy`, `advances`). Each is read only while the entry's
    * `ledger.advancedBy` is the recorded boundary's, so a bound manifest
    * that lags the ledger proves nothing.
    */
   boundEntries?: ReadonlyMap<
      string,
      Pick<ManifestEntry, "builtAt" | "ledger">
   >;
}

/** The `LedgerEntry` keys this release reads; any other key is an extension. */
const KNOWN_LEDGER_KEYS: ReadonlySet<string> = new Set([
   "connectionName",
   "physicalTableName",
   "storageDestinationName",
   "coveredThrough",
   "coveredThroughType",
   "watermark",
   "mergeKeys",
   "strategy",
   "sourceEntityId",
]);

/** The keys of a wire ledger entry this release does not know, or undefined. */
function ledgerExtensions(
   entry: LedgerEntry,
): Record<string, unknown> | undefined {
   const extra = Object.entries(entry).filter(
      ([key]) => !KNOWN_LEDGER_KEYS.has(key),
   );
   return extra.length > 0 ? Object.fromEntries(extra) : undefined;
}

/**
 * Index a caller-supplied ledger by table (the same key the local store uses),
 * converting each wire entry into the boundary shape the planner reads. Every
 * field is one the publisher reported on a manifest entry, echoed back; keys
 * this release does not know ride along as `extensions`, untouched.
 */
export function indexCallerLedger(
   entries: LedgerEntry[],
): Map<string, RecordedBoundary> {
   const index = new Map<string, RecordedBoundary>();
   for (const entry of entries) {
      const extensions = ledgerExtensions(entry);
      index.set(
         recordedTableKey({
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
            ...(extensions ? { extensions } : {}),
         },
      );
   }
   return index;
}

/**
 * A source's incremental refresh in this run: the run's context with the
 * lineage the source's declaration implies, or undefined when the run declared
 * none, the source has no content address, or it is not a delta candidate
 * ({@link incrementalLineage}). A table in a storage destination is written in
 * DuckDB; a database one in the source's own dialect. The build dispatch and
 * the skip-if-unchanged check share this one predicate so they never disagree.
 */
export function sourceIncrementalRefresh(
   context: IncrementalRunContext | undefined,
   persistSource: Pick<
      PersistSource,
      "sourceID" | "dialectName" | "connectionName"
   >,
   table: {
      physicalTableName: string;
      storageDestinationName?: string;
      sourceEntityId: string | undefined;
      /** For an incremental extension table: the parent it follows. */
      parent?: ExtensionParent;
   },
): { context: IncrementalRunContext; lineage: IncrementalLineage } | undefined {
   if (!context || !table.sourceEntityId) return undefined;
   const lineage = incrementalLineage({
      declaration: context.declarations[persistSource.sourceID],
      dialect: persistSource.dialectName,
      targetDialect:
         table.storageDestinationName !== undefined
            ? STORAGE_DIALECT
            : persistSource.dialectName,
      physicalTableName: table.physicalTableName,
      connectionName: persistSource.connectionName,
      storageDestinationName: table.storageDestinationName,
      sourceEntityId: table.sourceEntityId,
   });
   if (!lineage) return undefined;
   return {
      context,
      lineage: table.parent ? { ...lineage, parent: table.parent } : lineage,
   };
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
    * The dialect the DML will be issued in — the source's own for a database
    * table, `duckdb` for one in a `storage=` destination.
    */
   targetDialect: string;
   physicalTableName: string;
   connectionName: string;
   /**
    * The storage destination the table lives in, absent when database. Part of
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
   if (!capabilitiesOf(params.dialect).incrementalSourceRange) return undefined;
   // The DML engine must be one whose transactional apply is proven. A legal
   // `storage=` destination is always DuckDB-family; this states the invariant
   // where it is relied on.
   if (!capabilitiesOf(params.targetDialect).incrementalTargetDml) {
      return undefined;
   }
   const mergeKeys = d.mergeKeys
      .filter((k) => k.kind === "dimension")
      .map((k) => k.name);
   if (mergeKeys.length !== d.mergeKeys.length) return undefined;
   // Backstop behind the publish-time refusal: a merge whose scope could not be
   // resolved must never be ISSUED. No lineage means a seed: slower, but correct.
   if (mergeKeys.length > 0 && d.scopeIncomplete) return undefined;
   // A pair table's delta replaces whole parents by its key, in DuckDB, the
   // target of every `storage=` table and of a database table in a DuckDB
   // warehouse (pairDeltaStatements). Anything else seeds: a database pair
   // table in another warehouse, which its plan already rebuilds in full with
   // a package warning (`materialize-pair-full-refresh`), so this is only a
   // backstop, or a pair whose merge key is not its parent key, which was
   // planned wrong.
   if (
      d.pair &&
      (!capabilitiesOf(params.targetDialect).pairDelta ||
         mergeKeys.length !== 1 ||
         mergeKeys[0] !== d.pair.parentKey)
   ) {
      return undefined;
   }
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
      ...(d.rollingWindow?.length ? { rollingWindow: d.rollingWindow } : {}),
      ...(d.pair ? { pair: d.pair } : {}),
      ...(d.lookback?.seconds ? { lookbackSeconds: d.lookback.seconds } : {}),
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
    * {@link warehouseDeltaTarget} for a database source, the storage target for
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
   /** The source's name, for what the run reports about it. */
   sourceName?: string;
}): Promise<IncrementalStep> {
   const { context, lineage, target } = params;
   const forceRefresh = context.forceRefresh || params.reseed === true;

   let ledgerEntry: RecordedBoundary | null = null;
   if (context.callerLedger) {
      // No fallback to the local store for a table the caller omitted: a
      // worker's own row may be a generation stale, which is why this mode
      // exists. No entry means the ordinary `no_boundary` seed, unless the
      // caller sent one the controller dropped as malformed.
      ledgerEntry = context.callerLedger.get(recordedTableKey(lineage)) ?? null;
      const defect = context.malformedCallerLedger?.get(
         lineage.physicalTableName,
      );
      if (ledgerEntry === null && defect !== undefined && !forceRefresh) {
         return {
            mode: "seed",
            reasonCode: "ledger_entry_malformed",
            reason: `the supplied ledger entry for this table was malformed (${defect}) and was dropped`,
         };
      }
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
      capabilitiesOf(target.dialect).mergeGatedByServerVersion &&
      ledgerEntry !== null
         ? await probeServerVersion(target.runner, target.dialect)
         : undefined;
   const upstream = coordinateWithParent({
      lineage,
      recorded: ledgerEntry,
      read: lastReadParent(context, lineage, ledgerEntry),
      step: lineage.parent
         ? context.parentSteps?.get(lineage.parent.address)
         : undefined,
   });
   if (upstream?.reseed?.overflow && lineage.parent) {
      logger.warn(
         "Rebuilding an incremental extension in full: its parent was " +
            "refreshed more often since it last read the parent's table " +
            "than the parent's ledger lists",
         {
            packageName: context.packageName,
            physicalTableName: lineage.physicalTableName,
            parentName: lineage.parent.name,
            listed: LEDGER_ADVANCES_CAP,
            code: MATERIALIZE_EXTENSION_GENERATION_UNPROVEN_CODE,
         },
      );
   }
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
         ...(upstream ? { upstream } : {}),
      });
   } catch (err) {
      return {
         mode: "seed",
         reasonCode: "plan_error",
         reason: `the delta could not be planned (${errMessage(err)})`,
      };
   }
   noteStep(context, lineage, ledgerEntry, step, params.sourceName);
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
 * Which build of its parent's table an extension's recorded boundary was
 * reached over: the caller's ledger entry's `parent`; standalone, the bound
 * manifest's record for the extension's address. Undefined for a source with
 * no parent, or when neither records it.
 */
function lastReadParent(
   context: IncrementalRunContext,
   lineage: IncrementalLineage,
   recorded: RecordedBoundary | null,
): LedgerParent | undefined {
   const parent = lineage.parent;
   if (!parent) return undefined;
   const echoed = recordedParent(recorded);
   if (echoed?.sourceEntityId === parent.address) return echoed;
   if (context.callerLedger) return undefined;
   return context.boundParentReads
      ?.get(lineage.sourceEntityId)
      ?.find((read) => read.sourceEntityId === parent.address);
}

/** A string extension key of an echoed ledger entry, when it is one. */
function stringKey(
   extensions: Readonly<Record<string, unknown>> | undefined,
   key: string,
): string | undefined {
   const value = extensions?.[key];
   return typeof value === "string" && value !== "" ? value : undefined;
}

/**
 * The `parent` extension key of an echoed ledger entry, when it has the
 * shape the publisher writes; anything else reads as absent (no record).
 */
function recordedParent(
   recorded: RecordedBoundary | null,
): LedgerParent | undefined {
   const raw = recorded?.extensions?.parent;
   if (typeof raw !== "object" || raw === null) return undefined;
   const p = raw as Record<string, unknown>;
   if (typeof p.sourceEntityId !== "string") return undefined;
   const text = (k: string) =>
      typeof p[k] === "string" ? { [k]: p[k] as string } : {};
   return {
      sourceEntityId: p.sourceEntityId,
      ...text("advancedBy"),
      ...text("seededBy"),
      ...text("builtAt"),
      ...text("coveredThrough"),
      ...text("coveredThroughType"),
   };
}

/** The run that last changed a recorded boundary's table, when known. */
function recordedAdvancedBy(
   recorded: RecordedBoundary | null,
): string | undefined {
   if (!recorded) return undefined;
   return (
      stringKey(recorded.extensions, "advancedBy") ??
      (recorded as { advancedByMaterializationId?: string })
         .advancedByMaterializationId
   );
}

/**
 * The run that last changed a table its refresh left in force: this run's,
 * for a delta or a rebuild; the recorded one, for a refresh that applied
 * nothing. Reported on the ledger entry (`advancedBy`).
 */
export function advancedByOf(
   context: IncrementalRunContext,
   lineage: IncrementalLineage,
   did: "delta" | "full" | "none" | undefined,
): string | undefined {
   if (did !== "none") return context.materializationId;
   return context.skippedAdvancedBy?.get(lineage.sourceEntityId);
}

/**
 * Keep a source's planned step for the extensions over it, and record an
 * extension rebuilt because no record named its parent's build it last read
 * (logged and warned, so a reseed on every run is not silent).
 */
function noteStep(
   context: IncrementalRunContext,
   lineage: IncrementalLineage,
   recorded: RecordedBoundary | null,
   step: IncrementalStep,
   sourceName: string | undefined,
): void {
   const priorAdvancedBy = recordedAdvancedBy(recorded);
   const bound = context.boundEntries?.get(lineage.sourceEntityId);
   const boundIsPrior =
      priorAdvancedBy !== undefined &&
      bound?.ledger?.advancedBy === priorAdvancedBy;
   // The caller's echoed entry carries its history; the local store keeps
   // none, so a standalone run reads it off the bound entry.
   const priorChain = recorded
      ? context.callerLedger
         ? readAdvanceChain(recorded.extensions)
         : boundIsPrior
           ? readAdvanceChain(bound?.ledger as Record<string, unknown>)
           : undefined
      : undefined;
   context.parentSteps?.set(lineage.sourceEntityId, {
      mode: step.mode,
      ...(recorded
         ? {
              prior: {
                 malloyType: recorded.coveredThroughType,
                 value: recorded.coveredThroughValue,
              },
           }
         : {}),
      ...(priorAdvancedBy ? { priorAdvancedBy } : {}),
      ...(step.mode === "delta" ? { start: step.start } : {}),
      ...(priorChain ? { priorChain } : {}),
      ...(boundIsPrior && bound?.builtAt
         ? { priorBuiltAt: bound.builtAt }
         : {}),
   });
   if (step.mode === "skip" && priorAdvancedBy) {
      (context.skippedAdvancedBy ??= new Map()).set(
         lineage.sourceEntityId,
         priorAdvancedBy,
      );
   }
   const parent = lineage.parent;
   if (
      parent &&
      step.mode === "seed" &&
      step.reasonCode === "upstream_generation_skipped" &&
      lastReadParent(context, lineage, recorded) === undefined
   ) {
      logger.warn(
         "Rebuilding an incremental extension in full: no record says which " +
            "build of its parent's table it last read",
         {
            packageName: context.packageName,
            physicalTableName: lineage.physicalTableName,
            parentName: parent.name,
            code: MATERIALIZE_EXTENSION_GENERATION_UNPROVEN_CODE,
         },
      );
      (context.unprovenGenerations ??= []).push({
         sourceName: sourceName ?? lineage.physicalTableName,
         sourceEntityId: lineage.sourceEntityId,
         parentName: parent.name,
      });
   }
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
         code: errorCodeOf(err),
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
         code: errorCodeOf(err),
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
   /** The column the table holds the watermark under, when renamed. */
   storedColumn?: (field: string) => string;
}): Promise<WatermarkBound | undefined> {
   const boundary = await seedCoveredThrough(
      params.runner,
      params.dialect,
      params.lineage,
      params.quotedTablePath,
      params.context.now,
      params.storedColumn,
   );
   if (!boundary.bound) {
      if (boundary.error) {
         logger.warn(
            "Could not read a covered_through boundary after a full rebuild; " +
               "the next refresh will rebuild again",
            {
               packageName: params.context.packageName,
               physicalTableName: params.lineage.physicalTableName,
               // The code only: the error can quote the watermark value read.
               errorCode: boundary.errorCode,
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
 * Apply the step a plan decided on and report what happened, for both database
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
   /** What the late-row check found below the delta (see reportLateRows). */
   lateRows?: LateRows;
}> {
   const { context, lineage, step, target } = params;
   const carried = context.priorLateRows?.get(lineage.sourceEntityId);
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
         // Nothing was rebuilt, so what was missing still is.
         ...(carried ? { lateRows: carried } : {}),
      };
   }

   const startTime = performance.now();
   // One call, so the range replace's DELETE and INSERT commit or roll back
   // together (see deltaScript). A failure leaves the table and the boundary
   // as they were; the next run recomputes the same range.
   try {
      await applyDeltaScript(
         target.runner,
         target.dialect,
         step.statements,
         target.abortSignal,
      );
   } catch (err) {
      // A pair delta whose parent-row check failed was rolled back: rebuild
      // in full, which writes the flag over the whole table (fail safe).
      // A delta value its DECIMAL column cannot hold exactly was refused, not
      // rounded: rebuild in full, which widens the column to hold it.
      const decimal = isDecimalDeltaGuardFailure(err);
      if (!decimal && (!lineage.pair || !isPairGuardFailure(err))) throw err;
      reportIncrementalStep({
         step: decimal
            ? {
                 mode: "seed",
                 // The stored column's shape cannot hold the delta: no metric
                 // label of its own, the reason names the decimal cause.
                 reasonCode: "shape_mismatch",
                 reason:
                    "a delta value needs more digits than its stored DECIMAL " +
                    "column holds, so it was rolled back rather than rounded " +
                    "and the table rebuilds at the wider scale",
              }
            : {
                 mode: "seed",
                 reasonCode: "pair_parent_row",
                 reason:
                    "the delta would leave a parent without exactly one " +
                    "parent-row flag, so it was rolled back",
              },
         sourceName: params.sourceName,
         packageName: context.packageName,
         physicalTableName: lineage.physicalTableName,
      });
      return { applied: false };
   }
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
      durationMs,
   });
   // After the commit and the advance, and outside the apply's duration: the
   // check reads, never writes, and its failure costs only the report.
   const lateRows = target.abortSignal?.aborted
      ? carried
      : await reportLateRows({
           target,
           lineage,
           step,
           carried,
           sourceName: params.sourceName,
           packageName: context.packageName,
        });
   return {
      applied: true,
      durationMs,
      coveredThrough: step.coveredThrough,
      refresh: "delta",
      ...(lateRows ? { lateRows } : {}),
   };
}

/**
 * Run the late-row check for a committed delta ({@link countLateRows}) and log
 * what it found, under {@link INCREMENTAL_LATE_ROWS_CODE}: the rows the source
 * holds just below the delta's lower bound that the stored table lacks, so the
 * table undercounts until a rebuild. The window's bounds are watermark values
 * from the data, so they ride the manifest entry, not the log.
 */
async function reportLateRows(params: {
   target: DeltaTarget;
   lineage: IncrementalLineage;
   step: Extract<IncrementalStep, { mode: "delta" }>;
   /** What earlier deltas found, carried until a rebuild. */
   carried: LateRows | undefined;
   sourceName: string;
   packageName: string;
}): Promise<LateRows | undefined> {
   const { lineage, step } = params;
   const found = await countLateRows({
      target: params.target,
      lineage,
      start: step.start,
      end: step.end,
      carried: params.carried,
   });
   const at = {
      packageName: params.packageName,
      sourceName: params.sourceName,
      physicalTableName: lineage.physicalTableName,
   };
   if (found.error !== undefined) {
      logger.warn("Could not check an incremental delta for late rows", {
         ...at,
         // The code only: the error can quote a watermark value.
         errorCode: found.errorCode,
      });
      return found.lateRows;
   }
   if (found.lateRows && found.lateRows !== params.carried) {
      logger.warn(
         "An incremental source's table is missing rows stamped below its " +
            "delta's lower bound",
         {
            ...at,
            code: INCREMENTAL_LATE_ROWS_CODE,
            count: found.lateRows.count,
         },
      );
   }
   return found.lateRows;
}

/**
 * The manifest entry's `lateRows` field: what the late-row check found below
 * this run's delta, or nothing when it found nothing or did not run.
 */
export function lateRowsFields(
   lateRows: LateRows | undefined,
): { lateRows: LateRows } | Record<never, never> {
   return lateRows ? { lateRows } : {};
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
   // quietly being rebuilt in full every run. A skip because no new data
   // arrived is the steady state of a schedule: info. The code only: the
   // free-text reason can quote a watermark boundary, which is a value from
   // the data, or a driver's error. The boundary in force is on the manifest
   // entry.
   const level =
      step.mode === "skip" && step.reasonCode === "not_advanced"
         ? "info"
         : "warn";
   logger[level](
      step.mode === "seed"
         ? "Rebuilding an incremental source in full"
         : "Skipping an incremental source's refresh",
      {
         packageName,
         sourceName,
         physicalTableName,
         reasonCode: step.reasonCode,
      },
   );
}

/**
 * Count and log a delta that COMMITTED. With {@link reportIncrementalStep}, every
 * step is reported exactly once. The range is not logged: its bounds are
 * watermark values, from the data. The boundary reached is on the manifest
 * entry.
 */
function reportDeltaApplied(params: {
   sourceName: string;
   packageName: string;
   physicalTableName: string;
   durationMs: number;
}): void {
   recordIncrementalStep("delta");
   logger.debug("Applied an incremental delta", params);
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
 *
 * `extensions` are publisher-owned keys beyond the ones listed here, normally
 * {@link ledgerExtensionKeys}: the keys carried from the caller's echoed
 * entry, amended with the ones this run computed. The entry is opaque to hosts, which return it whole. A
 * known key always wins over an extension of the same name.
 */
export function ledgerFields(
   lineage: IncrementalLineage | undefined,
   bound: WatermarkBound | undefined,
   extensions?: Readonly<Record<string, unknown>>,
): { ledger: LedgerEntry } | Record<never, never> {
   if (!lineage || !bound) return {};
   return {
      ledger: {
         ...extensions,
         connectionName: lineage.connectionName,
         physicalTableName: lineage.physicalTableName,
         // Absent (not undefined) for a database table: absence MEANS database
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
/**
 * The publisher's own extension keys on the ledger entry a run reports (see
 * {@link ledgerFields}'s `extensions`), the one way they are carried:
 *  - the keys this release does not know on the caller's echoed entry for
 *    the table, carried as sent, except when the run reads the local store,
 *    the caller sent no entry, or the source is reseeded (its entry ignored);
 *  - amended with the keys this run computes, which never come from the
 *    echoed entry: `advancedBy`, the run that last changed the table, and on
 *    an incremental extension table `parent`, the build of its parent's
 *    table the boundary was reached over. An echoed one is dropped when the
 *    run computes none, so a stale value is never reported again.
 * A host stores and returns them with the rest of the entry.
 */
export function ledgerExtensionKeys(
   refresh:
      | { context: IncrementalRunContext; lineage: IncrementalLineage }
      | undefined,
   reseed: boolean | undefined,
   advancedBy: string | undefined,
   did: "delta" | "full" | "none" | undefined,
): Record<string, unknown> | undefined {
   const {
      advancedBy: _carriedAdvancedBy,
      parent: _carriedParent,
      seededBy: _carriedSeededBy,
      advances: _carriedAdvances,
      ...carried
   } = carriedLedgerExtensions(refresh, reseed) ?? {};
   const parent = ledgerParentOf(refresh?.lineage);
   const chain = refresh ? nextAdvanceChain(refresh, did) : undefined;
   const keys: Record<string, unknown> = {
      ...carried,
      ...(advancedBy ? { advancedBy } : {}),
      ...(parent ? { parent } : {}),
      ...(chain ? { seededBy: chain.seededBy, advances: chain.advances } : {}),
   };
   return Object.keys(keys).length > 0 ? keys : undefined;
}

/**
 * A table's history since its last full rebuild after this run's refresh
 * (the ledger's `seededBy` and `advances`, domain/incremental.ts): a full
 * rebuild starts it over; a refresh that applied nothing keeps it; a delta
 * appends itself, keeping the newest {@link LEDGER_ADVANCES_CAP}. A delta
 * over a recorded history that does not end at the build it started from
 * (or none, an entry from before these keys) starts one anchored at that
 * build, so an extension that read earlier rebuilds rather than trusting it.
 * Undefined when nothing proves it.
 */
function nextAdvanceChain(
   refresh: { context: IncrementalRunContext; lineage: IncrementalLineage },
   did: "delta" | "full" | "none" | undefined,
): LedgerAdvanceChain | undefined {
   const { context, lineage } = refresh;
   if (did === "full") {
      return { seededBy: context.materializationId, advances: [] };
   }
   const step = context.parentSteps?.get(lineage.sourceEntityId);
   if (did === "none") return step?.priorChain;
   if (did !== "delta" || !step?.priorAdvancedBy || !step.start) {
      return undefined;
   }
   const prior = step.priorChain;
   const last = prior?.advances.at(-1)?.advancedBy ?? prior?.seededBy;
   const base: LedgerAdvanceChain =
      prior && last === step.priorAdvancedBy
         ? prior
         : { seededBy: step.priorAdvancedBy, advances: [] };
   return {
      seededBy: base.seededBy,
      advances: [
         ...base.advances,
         {
            from: step.priorAdvancedBy,
            advancedBy: context.materializationId,
            start: step.start.value,
            startType: step.start.malloyType,
         },
      ].slice(-LEDGER_ADVANCES_CAP),
   };
}

/**
 * The keys this release does not know on the caller's echoed entry for this
 * source's table, as sent: a fresh object. Undefined when the run reads the
 * local store, the caller sent no entry for the table or one with no such
 * keys, or the source is reseeded, whose entry is ignored. Read through
 * {@link ledgerExtensionKeys}, which amends it.
 */
function carriedLedgerExtensions(
   refresh:
      | { context: IncrementalRunContext; lineage: IncrementalLineage }
      | undefined,
   reseed: boolean | undefined,
): Record<string, unknown> | undefined {
   if (!refresh || reseed || refresh.context.forceRefresh) return undefined;
   const carried = refresh.context.callerLedger?.get(
      recordedTableKey(refresh.lineage),
   )?.extensions;
   return carried ? { ...carried } : undefined;
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
   compiled: RunPlan,
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
      /** The caller's entries dropped as malformed; read only beside `ledger`. */
      malformedLedger?: MalformedLedgerEntry[];
   },
   localLedger: IncrementalLedgerStore,
): IncrementalRunContext | undefined {
   // collectIncrementalDeclarations drops (and warns on) an unreadable
   // declaration, so that source builds full.
   const declarations: Record<string, IncrementalDeclaration> = {};
   for (const [sourceID, declaration] of Object.entries(
      collectIncrementalDeclarations(compiled.tables),
   )) {
      if (declaration.incremental) declarations[sourceID] = declaration;
   }
   if (Object.keys(declarations).length === 0) return undefined;
   const { ledger, malformedLedger, ...rest } = run;
   return {
      ...rest,
      declarations,
      parentSteps: new Map(),
      ledger: localLedger,
      ...(ledger !== undefined
         ? { callerLedger: indexCallerLedger(ledger) }
         : {}),
      ...(ledger !== undefined && malformedLedger?.length
         ? {
              malformedCallerLedger: new Map(
                 malformedLedger.map((m) => [m.physicalTableName, m.defect]),
              ),
           }
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
      sourceName: persistSource.name,
   });
   const outcome = await applyIncrementalStep({
      context,
      lineage,
      step,
      target,
      sourceName: persistSource.name,
   });
   if (!outcome.applied) return undefined;

   // A delta or skip leaves the existing table serving; it must still enter the
   // build manifest, or a downstream source in this run would recompute it from
   // raw.
   params.manifest.update(sourceEntityId, {
      tableName: target.quotedTablePath,
   });
   // A database table's warehouse column types, read again as a full build
   // reads them (database_native_types.ts): the entry replaces the last
   // build's, and without them its big-integer columns would answer live.
   const nativeSchema =
      lineage.storageDestinationName === undefined
         ? await capturedNativeSchema({
              run: target.runner,
              physicalTableName: target.logicalTablePath,
              dialect: target.dialect,
              sourceName: persistSource.name,
           })
         : undefined;
   type ReadConnection = Pick<ReadConnectionSource, "runSQL">;
   // Its column contract, read again after the delta: a delta writes in
   // place, outside any transaction, and never changes the column set, so a
   // read that fails fails the source after the write (the previous entry
   // understates the table's freshness, never its rows), and a retry reads
   // it again.
   const contract =
      lineage.storageDestinationName === undefined
         ? await captureDatabaseContract({
              connection: {
                 dialectName: target.dialect,
                 runSQL: (sql, options) =>
                    target.runner(sql, options) as ReturnType<
                       ReadConnection["runSQL"]
                    >,
              },
              tableName: target.logicalTablePath,
              physicalTableName: lineage.physicalTableName,
              declared: persistSourceDeclaredColumns(persistSource),
           })
         : undefined;
   return {
      sourceEntityId,
      sourceName: persistSource.name,
      materializedTableId: instruction.materializedTableId,
      physicalTableName: lineage.physicalTableName,
      connectionName: persistSource.connectionName,
      realization: "COPY",
      ...params.upstreamReuse,
      ...(contract ? { contract } : {}),
      rowCount: null,
      buildDurationMs: outcome.durationMs ?? null,
      // applyDeltaScript returns no cost figure, even on backends that have one.
      queryCostBytes: null,
      // A delta reports where it advanced to; a skip, the boundary in force.
      ...ledgerFields(
         lineage,
         outcome.coveredThrough,
         ledgerExtensionKeys(
            { context, lineage },
            instruction.reseed,
            advancedByOf(context, lineage, outcome.refresh),
            outcome.refresh,
         ),
      ),
      ...refreshFields(outcome.refresh),
      ...lateRowsFields(outcome.lateRows),
      ...(nativeSchema?.length ? { nativeSchema } : {}),
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
         const { target, readCost, numericColumnsStoredAsDouble } =
            storageDeltaTarget({
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
         const reseeding = instruction.reseed || context.forceRefresh;
         // A table an older build typed (incremental_recast.spec.ts): a delta
         // only INSERTs into the columns it has, so only a re-seed's CTAS casts
         // them.
         const recast = await columnsARebuildWouldRecast(
            session,
            quotedTablePath,
            persistSource,
         );
         // A Postgres numeric a build before the decimal-precision fix stored
         // as DOUBLE, which serves float sums until the table is seeded again
         // (passthrough/postgres.ts); a seed now stores it exactly.
         const doubles = reseeding ? [] : await numericColumnsStoredAsDouble();
         // The stored table's columns, under the names the delta's SQL
         // emits them (a renamed field's column under its own name), so a
         // delta of a source with a `rename:` matches the table.
         const toStored = storedColumnOf(persistSource);
         let step = await planSourceRefresh({
            context,
            lineage,
            target,
            columns: deriveColumns(persistSource).map((c) =>
               toStored(String(c.name)),
            ),
            reseed:
               instruction.reseed || recast.length > 0 || doubles.length > 0,
            sourceName: persistSource.name,
         });
         const recastOnly =
            step.mode === "seed" && step.reasonCode === "forced" && !reseeding;
         if (recastOnly && doubles.length > 0) {
            // No metric label of its own: the stored column's shape cannot
            // hold the source's exact values, as a decimal delta that does
            // not fit re-seeds, and the reason names the cause.
            step = {
               mode: "seed",
               reasonCode: "shape_mismatch",
               reason:
                  `the stored table holds the Postgres numeric ${doubles
                     .map((c) => `\`${c}\``)
                     .join(", ")} as DOUBLE, which serves float sums ` +
                  `where live is exact; a rebuild stores it as DECIMAL, ` +
                  `and a delta cannot change a column's type`,
            };
            logger.info("Re-seeding to store Postgres numerics exactly", {
               code: STORED_COLUMN_TYPE_MISMATCH_CODE,
               sourceName: persistSource.name,
               columns: doubles,
            });
         } else if (recastOnly && recast.length > 0) {
            step = {
               mode: "seed",
               reasonCode: "forced",
               reason:
                  `the stored table holds ${recast
                     .map((m) => `\`${m.column}\` as ${m.storedType}`)
                     .join(
                        ", ",
                     )}, which a rebuild stores as the declared type, ` +
                  `and a delta cannot change a column's type`,
            };
            logger.info("Re-seeding to store declared column types", {
               code: STORED_COLUMN_TYPE_MISMATCH_CODE,
               sourceName: persistSource.name,
               columns: recast,
            });
         }
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
            ...(outcome.lateRows ? { lateRows: outcome.lateRows } : {}),
         };
      },
      afterSeed: ({ session, quotedTablePath }) =>
         advanceLedgerAfterSeed({
            context,
            lineage,
            quotedTablePath,
            dialect: STORAGE_DIALECT,
            runner: (sql) => session.runSQL(sql),
            storedColumn: storedColumnOf(persistSource),
         }),
   };
}

/**
 * The in-place refresh of a `storage=` extension table stacked on its
 * parent's table in its own destination (extension chains, case 2): a delta
 * of the extension's build over the parent's stored table, run and applied
 * in the destination session, or a rebuild. The same planner, ledger and
 * DML as any `storage=` table; only where the rows come from differs.
 */
export function stackedRefreshFor(params: {
   context: IncrementalRunContext;
   lineage: IncrementalLineage;
   persistSource: PersistSource;
   instruction: BuildInstruction;
   destinationName: string;
   destinationSchema?: string;
   physicalTableName: string;
}): StackedIncrementalRefresh {
   const { context, lineage, persistSource, instruction } = params;
   return {
      plan: async ({ session, quotedTablePath, storedSQL }) => {
         const target = stackedDeltaTarget({
            session,
            destinationName: params.destinationName,
            destinationSchema: params.destinationSchema,
            physicalTableName: params.physicalTableName,
            quotedTablePath,
            lineage,
            persistSource,
            storedSQL,
         });
         const recast = await columnsARebuildWouldRecast(
            session,
            quotedTablePath,
            persistSource,
         );
         const toStored = storedColumnOf(persistSource);
         const step = await planSourceRefresh({
            context,
            lineage,
            target,
            columns: deriveColumns(persistSource).map((c) =>
               toStored(String(c.name)),
            ),
            reseed: instruction.reseed || recast.length > 0,
            sourceName: persistSource.name,
         });
         const outcome = await applyIncrementalStep({
            context,
            lineage,
            step,
            target,
            sourceName: persistSource.name,
         });
         if (!outcome.applied) {
            await resetLedger(context, lineage);
            return undefined;
         }
         return {
            durationMs: outcome.durationMs,
            coveredThrough: outcome.coveredThrough,
            refresh: outcome.refresh ?? "none",
            // Reads no warehouse: the parent's table is in the destination.
            readCost: null,
            ...(outcome.lateRows ? { lateRows: outcome.lateRows } : {}),
         };
      },
      afterSeed: ({ session, quotedTablePath }) =>
         advanceLedgerAfterSeed({
            context,
            lineage,
            quotedTablePath,
            dialect: STORAGE_DIALECT,
            runner: (sql) => session.runSQL(sql),
            storedColumn: storedColumnOf(persistSource),
         }),
   };
}

/**
 * The stored columns a rebuild would cast to their declared type, read from
 * the destination table (see rebuildWouldRecast). None when the table cannot be
 * described: a missing table seeds anyway.
 */
export async function columnsARebuildWouldRecast(
   session: { runSQL(sql: string): Promise<unknown> },
   quotedTablePath: string,
   persistSource: PersistSource,
): Promise<StoredColumnTypeMismatch[]> {
   let rows: Record<string, unknown>[];
   try {
      const result = (await session.runSQL(`DESCRIBE ${quotedTablePath}`)) as
         | Record<string, unknown>[]
         | { rows?: Record<string, unknown>[] };
      rows = Array.isArray(result) ? result : (result.rows ?? []);
   } catch {
      return [];
   }
   const schema = rows.flatMap((r) =>
      typeof r.column_name === "string" && typeof r.column_type === "string"
         ? [{ name: r.column_name, type: r.column_type }]
         : [],
   );
   return rebuildWouldRecast(
      schema,
      persistSourceDeclaredColumns(persistSource),
      { queryTimezone: persistSourceQueryTimezone(persistSource) },
   );
}
