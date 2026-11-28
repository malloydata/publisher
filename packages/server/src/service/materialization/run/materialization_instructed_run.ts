// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What one instructed run reads and records ({@link InstructedRun}): its fixed
 * inputs and the per-source outcomes its build loop accumulates. Also the
 * instruction lookup ({@link indexInstructions}, {@link resolveInstruction}),
 * the refusals made before anything is written
 * ({@link assertNoTableCollisions}, {@link assertInstructedDestinationWritable}),
 * and {@link recordInstructedBuildFailure}. Read by the materialization
 * service, the instructed build and run instructions.
 *
 * Writes nothing outside the run object; logs and counts instructions that
 * give one content address two tables, and logs two definitions on one table.
 * The checks throw before any table is touched.
 */

import { instructsUnwritableDestination } from "../domain/placement";
import type { Manifest, PersistSource } from "@malloydata/malloy";
import {
   BadRequestError,
   MaterializationEligibilityError,
   TableOwnershipError,
   ContractCaptureFailedError,
   SchemaIncompatibleError,
   UpstreamContractInsufficientError,
} from "../../../errors";
import { logger } from "../../../logger";
import type {
   BuildKind,
   BuildMethod,
   BuildTier,
   EligibilityRefusalReason,
} from "../../../materialization_metrics";
import {
   recordSharedAddressInstructions,
   recordTableCollision,
} from "../../../materialization_metrics";
import type {
   BuildInstruction,
   ManifestEntry,
   RefusedSource,
   SourceFailure,
} from "../domain/types";
import { errMessage } from "../../../utils";
import {
   getPersistCollisionEnforce,
   getPersistStorageMode,
} from "../../../config";
import { redactPgSecrets, redactStorageUrls } from "../../../pg_helpers";
import {
   addressSQL,
   computeSourceEntityId,
   declaredStorage,
   type RunPlan,
} from "./run_plan";
import { sourcesInBuildOrder } from "../domain/compiled_build_plan";
import {
   classifySourceFailure,
   strictMissReason,
   failedUpstreamsOf,
   type FailedUpstream,
} from "./build_failure_class";
import { redactConnectionSecrets } from "./connection_redaction";
import {
   type BuildEnvironment,
   type ChainedPlanFacts,
} from "./build_upstreams";
import { type BuildQueryMetadata } from "./build_query_tag";
import type { IncrementalRunContext } from "./incremental_build";
import type { UniquenessLedger } from "./inline_uniqueness";
import type { Materialization } from "../../../storage/DatabaseInterface";
import { tableNameParts, tableNamesMeet } from "../domain/table_name";
import {
   isPersistedResultSourceID,
   originOfTable,
} from "../domain/table_plans";
import { tableNamesFoldCase } from "../domain/storage_table_path";
import { reportSourceFailed } from "./run_telemetry";

/** One instructed run: what `InstructedBuild.run` reads. */
export interface InstructedBuildOptions {
   compiled: RunPlan;
   /**
    * The declared freshness window of each table, by address, at the run's
    * start: a rollup reads its base's stored table only when that table is
    * inside the rollup's window then (instructed_build.ts). Absent, no
    * window is declared anywhere.
    */
   freshness?: {
      windowSecondsAt(sourceEntityId: string): number | undefined;
      at: Date;
   };
   environment: BuildEnvironment;
   instructions: BuildInstruction[];
   seedEntries: Record<string, ManifestEntry>;
   signal: AbortSignal;
   /** Used only to reclaim this run's storage tables on failure. */
   owner?: { environmentId: string; packageName: string };
   buildMetadata?: BuildQueryMetadata;
   /** Undefined leaves every source on the full-rebuild path. */
   incremental?: IncrementalRunContext;
   /**
    * Auto-run refusals, named in the error of a run whose every instructed
    * source failed so it accounts for every unbuilt source.
    */
   priorRefusals?: RefusedSource[];
   /**
    * Tiers auto-run already cleared (see deriveSelfInstructions); a listed
    * source is not asserted again.
    */
   clearedTiers?: ReadonlyMap<string, EligibilityTier>;
   /**
    * The plan's own refusal of an instructed source the run's plan holds no
    * table for: the plan refused it (for example `upstream_table_unusable`,
    * judged against an upstream table the package had bound), so the build
    * loop never meets it. Its failure carries this refusal. An instruction
    * that matches no planned source and no refusal fails as
    * `instruction_unresolved`; none is dropped.
    */
   refusedAtPlan?: (instruction: BuildInstruction) => RefusedSource | undefined;
   /**
    * The host instructed this run (orchestrated): a host-supplied upstream
    * entry at the wrong tier is refused, not recomputed (see
    * ChainedPlanFacts.hostInstructed).
    */
   hostInstructed?: boolean;
   /**
    * Auto-run only: the pair tables this run rebuilds to run a leaf key
    * check their last build could not run, by content address
    * (deriveSelfInstructions). A check such a rebuild still cannot run is
    * recorded `rebuiltToVerify`, so the table is not rebuilt for it again.
    */
   rebuiltToVerify?: ReadonlySet<string>;
   /**
    * Auto-run only: the inline uniqueness checks' passes, read from the run
    * before and recorded for the run after (inline_uniqueness.ts). Undefined
    * runs every check as compiled.
    */
   uniqueness?: UniquenessLedger;
   /**
    * The package's runs, from its own records: what tells a table at a
    * `name=` this package wrote from anyone else's (table_ownership.ts).
    * Absent: no such table is ever recognized, and every one refuses.
    */
   packageRuns?: () => Promise<Materialization[]>;
   /**
    * Told each instruction whose build is about to write its table, before
    * the build starts, and whether the author chose its name (`name=`): a
    * run cancelled because its package is retiring gives back the tables
    * these builds may have written (Reclaimer.reclaimRetiredRunTables).
    */
   onBuildStarted?: (
      instruction: BuildInstruction,
      authorNamed: boolean,
   ) => void;
}

/** The eligibility gate a source was cleared by. */
export type EligibilityTier = "storage" | "database";

/**
 * Everything one instructed build run reads and records, in one place: the
 * run's fixed inputs, and the per-source outcomes the loop accumulates and the
 * run reports when it ends.
 */
export interface InstructedRun
   extends Pick<
      InstructedBuildOptions,
      | "compiled"
      | "environment"
      | "owner"
      | "buildMetadata"
      | "incremental"
      | "clearedTiers"
      | "packageRuns"
      | "uniqueness"
      | "freshness"
      | "onBuildStarted"
      | "rebuiltToVerify"
   > {
   planFacts: ChainedPlanFacts;
   index: InstructionIndex;
   manifest: Manifest;
   entries: Record<string, ManifestEntry>;
   // Entries this run actually CREATED, as opposed to the seeded/carried ones
   // above. Only these are eligible for failure-path reclaim: a carried entry
   // names a table an earlier successful run built and a live manifest may still
   // serve, so dropping one would be data loss rather than cleanup.
   builtThisRun: ManifestEntry[];
   // Stored tables this run wrote and deliberately will NOT reclaim, so a
   // failure can name them. See reportRetainedStorageTables for why the list
   // cannot say which of them is actually unreferenced.
   retainedThisRun: ManifestEntry[];
   failures: Record<string, SourceFailure>;
   /**
    * The instructions a planned source resolved to in the loop: every other
    * instruction is failed when the loop ends (recordUnresolvedInstructions),
    * so each instructed source ends in `entries` or `failures`.
    */
   resolvedInstructions: Set<BuildInstruction>;
   // The sources that failed (or were refused) so far, by sourceID, so a
   // dependent refused for want of one inherits its classification.
   failedBySourceID: Map<string, FailedUpstream>;
   // sourceID -> the sourceIDs on the tables it reads, across every build.
   upstreamsBySourceID: Map<string, string[]>;
   failedReasons: string[];
   refusals: MaterializationEligibilityError[];
   /**
    * Refusals not yet recorded against their address.
    * A refused source can share its content address with an eligible sibling
    * (a base and its `extend`); the refusal is decided after the loop, so it
    * fails the address only when no sibling built it, whatever the order.
    */
   pendingRefusals: PendingRefusal[];
   /**
    * Sources auto-run already refused (deriveSelfInstructions), by sourceID.
    * One sharing an eligible sibling's address still reaches the loop through
    * that sibling's instruction; it is skipped, already reported.
    */
   priorRefusedIDs: ReadonlySet<string>;
   /**
    * The planned sources on each content address, computed on first use (see
    * databaseRefusalOn). A source whose address cannot be computed is absent.
    */
   sourceIDsByAddress?: Map<string, string[]>;
   /**
    * Per content address, the sources on it the database gate refuses, in
    * sourceID order; decided on first use (see databaseRefusalOn).
    */
   databaseRefusalsByAddress?: Map<string, DatabaseRefusal[]>;
   /**
    * Set when a source its own gate admits was not built because a database
    * sibling on its table is refused; such a run is not failed as one whose
    * every source was refused.
    */
   heldBackBySibling?: boolean;
   /** Content addresses a source of this run built (not carried, not failed). */
   builtAddresses: Set<string>;
   builtSources: string[];
   // Keyed by physical table, not content address: the address says what a
   // table CONTAINS, the coordinate says which table it IS, and only the latter
   // answers "have I written this already".
   writtenTargets: Map<string, { sourceEntityId: string; sourceName: string }>;
   /**
    * Tables whose build failed this run, by the same key, with the error: a
    * second route to one takes that failure instead of building it again.
    */
   failedTargets?: Map<string, { sourceEntityId: string; err: unknown }>;
   /** The run's cancellation: a source failing after it is `cancelled`. */
   signal?: AbortSignal;
   /**
    * Tables built from a stored table the plan's edges do not name (a rollup
    * over its base's table), by address: what each read, for its freshness
    * anchor (freshness_stamp.ts).
    */
   readFrom?: Map<string, string[]>;
}

/** One source the database gate refuses, as databaseRefusalOn reports it. */
export interface DatabaseRefusal {
   sourceID: string;
   name: string;
   reason: EligibilityRefusalReason | undefined;
   message: string;
}

/** One gate refusal, held until the run knows whether its address was built. */
export interface PendingRefusal {
   /** The address the refusal would fail. */
   failedKey: string;
   failure: SourceFailure;
   /** The `ManifestEntry.error` mirror of `failure`, for the deprecation window. */
   errorEntry: ManifestEntry;
   /** The same refusal, per source, for the run's `refusedSources` metadata. */
   refused: RefusedSource;
   /**
    * The refused source's sourceID, so an address's recorded failure is
    * chosen in a stable order.
    */
   sourceID: string;
   /**
    * True when the source's own gate refused it; false when it is refused only
    * because a sibling on its database address is. An address's recorded
    * failure prefers an own refusal, so it names the gated source whatever the
    * run order.
    */
   own: boolean;
}

/** A run's instructions, indexed both ways a source can find its own. */
interface InstructionIndex {
   bySourceID: Map<string, BuildInstruction>;
   bySourceEntityId: Map<string, BuildInstruction>;
   /** The run's planned sources, when known, so a sourceID is judged by it. */
   planned?: Readonly<Record<string, unknown>>;
}

/** The instruction one source builds under, and how it was found. */
export interface ResolvedInstruction {
   instruction: BuildInstruction;
   /** Matched by `sourceID`, i.e. a caller-supplied (orchestrated) instruction. */
   orchestrated: boolean;
   /** The content address, when matching had to compute it. */
   sourceEntityId?: string;
}

export function indexInstructions(
   instructions: BuildInstruction[],
   planned?: Readonly<Record<string, unknown>>,
): InstructionIndex {
   // Indexed by sourceID first: sourceEntityId is opaque, caller-assigned
   // identity, and matching on a recomputed content hash would silently skip
   // every source of a caller that derives its ids by another scheme. The
   // sourceEntityId index is the fallback for instructions without a sourceID
   // (e.g. standalone auto-run).
   const bySourceID = new Map<string, BuildInstruction>();
   const bySourceEntityId = new Map<string, BuildInstruction>();
   for (const instruction of instructions) {
      if (instruction.sourceID) {
         bySourceID.set(instruction.sourceID, instruction);
      }
      // One address, several instructions naming DIFFERENT tables: a host that
      // mints a physical name per source produces this whenever sources share
      // one artifact (`#@ persist` is inherited; `extend` does not change the
      // materialization SQL). With `sourceID`s every named table is built and
      // all but the last go unreferenced; without them the last instruction
      // wins and the others are never built.
      //
      // Reported, not resolved: picking one would leave the host holding an
      // anchor for a table the build declined to write, which is worse and
      // invisible from its side. The fix belongs where the names are minted.
      const clash = bySourceEntityId.get(instruction.sourceEntityId);
      if (clash && clash.physicalTableName !== instruction.physicalTableName) {
         recordSharedAddressInstructions();
         logger.warn(
            "One content address was instructed to build more than one table",
            {
               sourceEntityId: instruction.sourceEntityId,
               physicalTableNames: [
                  clash.physicalTableName,
                  instruction.physicalTableName,
               ],
            },
         );
      }
      bySourceEntityId.set(instruction.sourceEntityId, instruction);
   }
   return { bySourceID, bySourceEntityId, ...(planned ? { planned } : {}) };
}

/**
 * The instruction that applies to one persist source, or undefined when the
 * run has nothing to build for it.
 */
export function resolveInstruction(
   index: InstructionIndex,
   persistSource: PersistSource,
   connectionDigests: RunPlan["connectionDigests"],
   tableFolds?: RunPlan["tableFolds"],
   upstreamSchemas?: RunPlan["upstreamSchemas"],
): ResolvedInstruction | undefined {
   // sourceID first, so the caller's sourceEntityId scheme stays opaque; the
   // sourceEntityId lookup is the fallback for instructions without one.
   const orchestratedInstruction = index.bySourceID.get(persistSource.sourceID);
   if (orchestratedInstruction) {
      return { instruction: orchestratedInstruction, orchestrated: true };
   }

   // Resolved BEFORE any eligibility assert runs: an uninstructed source (a
   // refused sibling the caller never asked to build) must be skipped, not
   // examined, or its 422 aborts the whole run.
   //
   // computeSourceEntityId throws for a free-parameter or given source, which
   // the wire build plan omits, so no caller can hold an id for it. Treat the
   // throw as "no match" rather than abort every other instructed source.
   let sourceEntityId: string;
   try {
      sourceEntityId = computeSourceEntityId(
         persistSource,
         connectionDigests,
         undefined,
         tableFolds,
         upstreamSchemas,
      );
   } catch {
      return undefined;
   }
   const instruction = index.bySourceEntityId.get(sourceEntityId);
   if (!instruction) return undefined;
   // The address is the fallback for an instruction WITHOUT a sourceID. One
   // that names another planned source was the host's for that source alone:
   // this one (an extension sharing its address) was not instructed, so it is
   // skipped. Its table is the named source's, which it reads by address
   // through its own gate; serving judges it from the plan, not this run.
   if (
      instruction.sourceID !== undefined &&
      instruction.sourceID !== persistSource.sourceID &&
      index.planned?.[instruction.sourceID] !== undefined
   ) {
      return undefined;
   }
   return { instruction, orchestrated: false, sourceEntityId };
}

/**
 * The plan's refusal of the source an instruction names: by its `sourceID`
 * when it carries one, else by its content address through the plan the
 * caller read (`served`), where a base and its `extend` share one address,
 * so every source on it is consulted, in sourceID order.
 */
export function planRefusalOf(
   served: Readonly<Record<string, { sourceEntityId: string }>>,
   refusals: Readonly<Record<string, RefusedSource>>,
): (instruction: BuildInstruction) => RefusedSource | undefined {
   const sourceIDsByAddress = new Map<string, string[]>();
   for (const [sourceID, source] of Object.entries(served)) {
      const ids = sourceIDsByAddress.get(source.sourceEntityId) ?? [];
      ids.push(sourceID);
      sourceIDsByAddress.set(source.sourceEntityId, ids);
   }
   return (instruction) => {
      if (instruction.sourceID !== undefined) {
         return refusals[instruction.sourceID];
      }
      const ids = sourceIDsByAddress.get(instruction.sourceEntityId) ?? [];
      for (const sourceID of [...ids].sort()) {
         if (refusals[sourceID]) return refusals[sourceID];
      }
      return undefined;
   };
}

/**
 * Two definitions on one physical table is refused BEFORE anything is
 * written. A mid-loop throw would leave the pair's FIRST table already
 * replaced, and nothing restores it: the failure path reclaims storage tables
 * only, and even a reclaimed one cannot restore the rows it overwrote.
 *
 * Not host-only: collisions are always warn-only at load whatever
 * PERSIST_COLLISION_ENFORCE says (see loadPackage), so a package with a
 * model-declared collision can still reach this build. Refused unless that
 * setting is `false` (it is on by default).
 */
export function assertNoTableCollisions(
   builds: RunPlan["builds"],
   tables: RunPlan["tables"],
   connectionDigests: RunPlan["connectionDigests"],
   index: InstructionIndex,
   tableFolds?: RunPlan["tableFolds"],
   upstreamSchemas?: RunPlan["upstreamSchemas"],
): void {
   // Keyed by the table segment on its connection or destination: `main.t`
   // and `t` may be one table, as the plan's collision check reads them
   // (tableNamesMeet).
   const claimedBy = new Map<
      string,
      {
         sourceName: string;
         sourceEntityId: string;
         table: string;
         sql?: string;
      }[]
   >();
   const collisions: {
      first: string;
      second: string;
      table: string;
      /** Same query, different stored columns (foldDistinctTableNames). */
      sameQuery: boolean;
   }[] = [];
   const sqlOf = (source: PersistSource) => {
      try {
         return addressSQL(source);
      } catch {
         return undefined;
      }
   };
   for (const build of builds) {
      for (const persistSource of sourcesInBuildOrder(build, tables)) {
         let address: string;
         try {
            address = computeSourceEntityId(
               persistSource,
               connectionDigests,
               undefined,
               tableFolds,
               upstreamSchemas,
            );
         } catch {
            // getSQL() throws for a source the eligibility gate is about to
            // refuse with a clean 422. That refusal is the build loop's to
            // give; a source with no readable address claims no table here.
            continue;
         }
         const instruction =
            index.bySourceID.get(persistSource.sourceID) ??
            index.bySourceEntityId.get(address);
         if (!instruction) continue;
         // Compared as the plan compares them: folded where the
         // destination folds table names to one case.
         const table = tableNamesFoldCase(
            persistSource.dialectName,
            !!instruction.destination,
         )
            ? instruction.physicalTableName.toLowerCase()
            : instruction.physicalTableName;
         const target = JSON.stringify([
            instruction.destination ? "destination" : "connection",
            instruction.destination ?? build.connectionName,
            tableNameParts(table).leaf,
         ]);
         const claims = claimedBy.get(target) ?? [];
         const sql = sqlOf(persistSource);
         const other = claims.find(
            (c) =>
               c.sourceEntityId !== address && tableNamesMeet(c.table, table),
         );
         if (other) {
            collisions.push({
               first: other.sourceName,
               second: persistSource.name,
               table: instruction.physicalTableName,
               sameQuery: sql !== undefined && sql === other.sql,
            });
         }
         claims.push({
            sourceName: persistSource.name,
            sourceEntityId: address,
            table,
            sql,
         });
         claimedBy.set(target, claims);
      }
   }
   for (const c of collisions) {
      recordTableCollision();
      logger.warn("Two definitions are materializing into one table", {
         physicalTableName: c.table,
         sourceNames: [c.first, c.second],
      });
   }
   if (collisions.length > 0 && getPersistCollisionEnforce()) {
      const detail = collisions
         .map(
            (c) =>
               `'${c.first}' and '${c.second}' ` +
               (c.sameQuery
                  ? `compile to the same query but store different columns, ` +
                    `yet both materialize`
                  : `compile to different SQL but both materialize`) +
               ` into table '${c.table}'`,
         )
         .join("; ");
      throw new MaterializationEligibilityError({
         message:
            `${detail}, so each would overwrite the other's rows while both ` +
            `resolve to it at serve time. Give them distinct definitions, or ` +
            `distinct physical names: a model-declared collision is fixed ` +
            `with '#@ persist name=', a host-assigned one by the caller that ` +
            `minted the names.`,
      });
   }
}

/**
 * Refuse an instruction whose destination cannot be written as instructed.
 * Thrown inside the per-source build, so it fails that source alone.
 */
export function assertInstructedDestinationWritable(
   persistSource: PersistSource,
   instruction: BuildInstruction,
   compiled: RunPlan,
): void {
   if (instructsUnwritableDestination(instruction, getPersistStorageMode())) {
      throw new BadRequestError(
         `Source '${persistSource.name}' was instructed to build into ` +
            `storage destination '${instruction.destination}', but ` +
            `PERSIST_STORAGE_MODE is off, so no destination can be ` +
            `written. Refusing rather than building the table into the ` +
            `source warehouse instead.`,
      );
   }

   // The source DECLARES `storage=` but the instruction carries no
   // destination (org not enabled, resolution failed, field omitted).
   // Unchecked, `isStorageBuild` is false in buildOneSource and the source is
   // CTAS'd into its own warehouse. `storage=` says where the data may be
   // written; it is not consent to a warehouse write (which a read-only
   // production credential usually cannot do, so the symptom would be a
   // permission error naming neither cause). Independent of the mode: no
   // reading of `storage=` makes the source warehouse right.
   //
   // Thrown per source, not per run, because these causes hit some sources
   // and not others; failing the run would take down unrelated database
   // sources too.
   const declared = declaredStorage(persistSource);
   if (declared && !instruction.destination) {
      throw new BadRequestError(
         `${declaredStorageSubject(persistSource, compiled, declared)}` +
            `, but the build was instructed with no storage ` +
            `destination, so there is nowhere it may be written. ` +
            `Refusing rather than building the table into the source ` +
            `warehouse, which the annotation asked it not to be ` +
            `written to.`,
      );
   }
}

/**
 * What declared a source's `storage=`, in the words its author wrote: the
 * planned table's `origin` decides it, never the `#@ persist storage=` line
 * the planner reads, which a `#(materialize)` source's build source and a
 * rollup carry synthesized. A rollup is named by its base and grain, as
 * `#(rollup)` (or v1's `#@ preaggregate`), whose grain renders as a set: the
 * wire plan sorts and dedupes dimensions, so quoting it would misrepresent the
 * author's order. A `#(materialize)` source by its own name, not its build
 * source's.
 */
function declaredStorageSubject(
   persistSource: PersistSource,
   compiled: RunPlan,
   declared: string,
): string {
   const table = compiled.tables[persistSource.sourceID];
   const origin = table
      ? originOfTable(table.kind, persistSource.sourceID)
      : "persist";
   switch (origin) {
      case "rollup": {
         const rollup = table?.rollup;
         const at = rollup
            ? `Measures of \`${rollup.baseSourceName}\` pre-aggregated at ` +
              `grain (${rollup.grainDimensions.join(", ")})`
            : `Rollup '${persistSource.name}'`;
         return (
            `${at} declare \`storage=${declared}\` on ` +
            `\`#(rollup)\` (or v1's \`#@ preaggregate\`)`
         );
      }
      case "materialize":
         return (
            `Source '${table?.materialize?.sourceName ?? persistSource.name}' ` +
            `declares \`#(materialize) storage=${declared}\``
         );
      case "view": {
         // A persisted view's build source is `<source>__<view>__view`.
         const [, source, view] =
            /^(.+)__(.+)__view$/.exec(persistSource.name) ?? [];
         return view
            ? `\`#@ persist storage=${declared}\` on view '${view}' of ` +
                 `source '${source}'`
            : `View source '${persistSource.name}' declares ` +
                 `\`#@ persist storage=${declared}\``;
      }
      case "query": {
         // A persisted named query's build source is `<query>__query`.
         const [, query] = /^(.+)__query$/.exec(persistSource.name) ?? [];
         return `\`#@ persist storage=${declared}\` on query '${query ?? persistSource.name}'`;
      }
      default:
         return (
            `Source '${persistSource.name}' declares ` +
            `\`#@ persist storage=${declared}\``
         );
   }
}

/** Record one instructed source's build failure on the run. */
export function recordInstructedBuildFailure(
   run: InstructedRun,
   persistSource: PersistSource,
   instruction: BuildInstruction,
   sourceEntityId: string,
   buildErr: unknown,
   /** What the source's build would make and how, for its build event. */
   build: {
      kind: BuildKind;
      tier: BuildTier;
      method: BuildMethod;
      durationMs?: number;
   },
): void {
   // One source failing does not end the build; the reason is recorded per
   // source so a consumer can attribute it. A build that loses every source
   // still fails.
   //
   // A warehouse error can echo credentials and this value is persisted, so
   // it is redacted against the connection CONFIG (where declared secrets
   // live), not the live connection: an accessor on a torn-down connection can
   // throw mid-walk and fail the run, and secrets held in a Map are never
   // walked. A connection missing from the environment makes
   // getApiConnection throw; fall back to the connection-free redactor rather
   // than leak.
   let reason: string;
   try {
      reason = redactConnectionSecrets(
         errMessage(buildErr),
         run.environment.getApiConnection(persistSource.connectionName),
      );
   } catch {
      reason = redactStorageUrls(redactPgSecrets(errMessage(buildErr)));
   }
   // Classified against the upstreams that already failed in this run: a
   // dependent refused because its upstream failed fails for the upstream's
   // reason, which it names, and inherits the upstream's classification.
   const failedUpstreams = failedUpstreamsOf(
      persistSource.sourceID,
      buildErr,
      run.upstreamsBySourceID,
      run.failedBySourceID,
   );
   const { retryable, upstreamDetail } = classifySourceFailure(
      buildErr,
      failedUpstreams,
   );
   // Malloy's strict manifest miss is the compiler's wording (a source id, a
   // build id): said instead as the upstream by name and file, and why it has
   // no table, with what Malloy adds after it read from the redacted
   // message. The upstreams' reasons were redacted when recorded.
   const missReason = strictMissReason(
      persistSource.name,
      buildErr,
      failedUpstreams,
      { message: reason, packagePath: run.environment.getPackagePath?.() },
   );
   if (missReason !== undefined) reason = missReason;
   else if (upstreamDetail) reason = `${reason}; ${upstreamDetail}`;
   run.failedBySourceID.set(persistSource.sourceID, {
      name: persistSource.name,
      reason,
      retryable,
   });
   // A build that lost a source does not throw, so without this event the
   // failure is invisible in the server's own output. The reason stays on the
   // run's record: a driver's message can quote SQL, so the log has its code.
   // Once per address, as the record counts it: a base and its `extend` share
   // one table, and the loop tries it again for the second.
   if (!run.failures[sourceEntityId]) {
      reportSourceFailed(
         {
            ...build,
            sourceEntityId,
            sourceName: persistSource.name,
            physicalTableName: instruction.physicalTableName,
            ...(instruction.destination
               ? { destination: instruction.destination }
               : {}),
         },
         buildErr,
         retryable,
         run.signal?.aborted ?? false,
      );
   }
   run.failedReasons.push(`${persistSource.name}: ${reason}`);
   run.failures[sourceEntityId] = {
      sourceEntityId,
      sourceName: persistSource.name,
      materializedTableId: instruction.materializedTableId,
      physicalTableName: instruction.physicalTableName,
      reason,
      // Omitted when the error is not recognized, so the consumer keeps its
      // own judgment, as for a Publisher that predates the field; and for a
      // source the run's cancel stopped, which did not fail (the run settles
      // CANCELLED, and its status is the signal).
      ...(retryable !== undefined && !run.signal?.aborted ? { retryable } : {}),
      // Lets a consumer tell a database failure from a `storage=` one; it
      // keys by `storageDestinationName ?? connectionName`, like a
      // successful ManifestEntry.
      connectionName: persistSource.connectionName,
      ...(instruction.destination
         ? { storageDestinationName: instruction.destination }
         : {}),
      // A bounded code beside the prose, for a refusal a client branches on.
      ...(buildErr instanceof TableOwnershipError ||
      buildErr instanceof ContractCaptureFailedError ||
      buildErr instanceof SchemaIncompatibleError ||
      buildErr instanceof UpstreamContractInsufficientError
         ? { code: buildErr.code }
         : {}),
      ...(buildErr instanceof UpstreamContractInsufficientError
         ? { staleUpstreams: [...buildErr.staleUpstreams] }
         : {}),
   };
   // Deprecated mirror into `entries` for consumers that read the failure
   // from `ManifestEntry.error`; without it they would see the source as
   // absent ("nothing to report"), the fail-dangerous reading. Overwrites
   // rather than merges: a seeded entry names the PRIOR generation's table,
   // which a serve binding would serve as fresh. Remove with the field.
   run.entries[sourceEntityId] = {
      sourceEntityId,
      sourceName: persistSource.name,
      physicalTableName: instruction.physicalTableName,
      materializedTableId: instruction.materializedTableId,
      error: reason,
   } as ManifestEntry;
}

/**
 * The record a run keeps for a source the eligibility gate refused, in the
 * plan's `RefusedSource` shape so both report it identically.
 */
export function refusalRecord(
   persistSource: PersistSource,
   err: MaterializationEligibilityError,
   tier: RefusedSource["tier"],
   modelPath: string | undefined,
   /** Its planned table's kind, which names its mechanism (`origin`). */
   kind?: Parameters<typeof originOfTable>[0],
): RefusedSource {
   return {
      origin:
         tier === "preaggregate"
            ? "rollup"
            : originOfTable(
                 kind ??
                    (isPersistedResultSourceID(persistSource.sourceID)
                       ? "view"
                       : "persist"),
                 persistSource.sourceID,
              ),
      name: persistSource.name,
      sourceID: persistSource.sourceID,
      ...(modelPath ? { modelPath } : {}),
      tier,
      reason: err.reason || "authorize",
      message: errMessage(err),
   };
}

/**
 * The run-level refusal when every targeted source was refused. Several
 * refusals are joined so the run names every source, not only the first.
 */
export function allSourcesRefusedError(
   errors: MaterializationEligibilityError[],
): MaterializationEligibilityError {
   if (errors.length === 1) return errors[0];
   const reason = errors.every((e) => e.reason === errors[0].reason)
      ? errors[0].reason
      : undefined;
   return new MaterializationEligibilityError({
      message: errors.map((e) => e.message).join("; "),
      reason,
   });
}
