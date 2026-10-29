// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Manifest, PersistSource } from "@malloydata/malloy";
import { BadRequestError, MaterializationEligibilityError } from "../errors";
import { logger } from "../logger";
import {
   recordSharedAddressInstructions,
   recordTableCollision,
} from "../materialization_metrics";
import type {
   BuildInstruction,
   ManifestEntry,
   SourceFailure,
} from "../storage/DatabaseInterface";
import { errMessage } from "../utils";
import { getPersistCollisionEnforce, getPersistStorageMode } from "../config";
import { redactPgSecrets } from "../pg_helpers";
import {
   computeSourceEntityId,
   sourcesInBuildOrder,
   type CompiledBuildPlan,
} from "./build_plan";
import type { IncrementalRunContext } from "./incremental_build";
import {
   declaredStorage,
   physicalTargetKey,
   redactConnectionSecrets,
   type BuildEnvironment,
   type BuildQueryMetadata,
   type ChainedPlanFacts,
} from "./materialization_run_support";

/** The eligibility gate a source was cleared by. */
export type EligibilityTier = "storage" | "colocated";

/**
 * Everything one instructed build run reads and records, in one place: the
 * run's fixed inputs, and the per-source outcomes the loop accumulates and the
 * run reports when it ends.
 */
export interface InstructedRun {
   compiled: CompiledBuildPlan;
   environment: BuildEnvironment;
   owner?: { environmentId: string; packageName: string };
   buildMetadata?: BuildQueryMetadata;
   incremental?: IncrementalRunContext;
   clearedTiers?: ReadonlyMap<string, EligibilityTier>;
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
   failedReasons: string[];
   refusals: MaterializationEligibilityError[];
   builtSources: string[];
   // Keyed by physical table, not content address: the address says what a
   // table CONTAINS, the coordinate says which table it IS, and only the latter
   // answers "have I written this already".
   writtenTargets: Map<string, { sourceEntityId: string; sourceName: string }>;
}

/** A run's instructions, indexed both ways a source can find its own. */
export interface InstructionIndex {
   bySourceID: Map<string, BuildInstruction>;
   bySourceEntityId: Map<string, BuildInstruction>;
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
   return { bySourceID, bySourceEntityId };
}

/**
 * The instruction that applies to one persist source, or undefined when the
 * run has nothing to build for it.
 */
export function resolveInstruction(
   index: InstructionIndex,
   persistSource: PersistSource,
   connectionDigests: CompiledBuildPlan["connectionDigests"],
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
      sourceEntityId = computeSourceEntityId(persistSource, connectionDigests);
   } catch {
      return undefined;
   }
   const instruction = index.bySourceEntityId.get(sourceEntityId);
   if (!instruction) return undefined;
   return { instruction, orchestrated: false, sourceEntityId };
}

/**
 * Two definitions on one physical table is refused BEFORE anything is
 * written. A mid-loop throw would leave the pair's FIRST table already
 * replaced, and nothing restores it: the failure path reclaims storage tables
 * only, and even a reclaimed one cannot restore the rows it overwrote.
 *
 * Not host-only: collisions are always warn-only at load whatever
 * PERSIST_COLLISION_ENFORCE says (see loadPackage), so a package with a
 * model-declared collision can still reach this build.
 */
export function assertNoTableCollisions(
   builds: CompiledBuildPlan["builds"],
   sources: CompiledBuildPlan["sources"],
   connectionDigests: CompiledBuildPlan["connectionDigests"],
   index: InstructionIndex,
): void {
   const claimedBy = new Map<
      string,
      { sourceName: string; sourceEntityId: string }
   >();
   const collisions: { first: string; second: string; table: string }[] = [];
   for (const build of builds) {
      for (const persistSource of sourcesInBuildOrder(build, sources)) {
         let address: string;
         try {
            address = computeSourceEntityId(persistSource, connectionDigests);
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
         // Must be the same key as the build loop's write guard, or this
         // pre-pass and the guard disagree on what a collision is.
         const target = physicalTargetKey(instruction, build.connectionName);
         const claim = claimedBy.get(target);
         if (!claim) {
            claimedBy.set(target, {
               sourceName: persistSource.name,
               sourceEntityId: address,
            });
         } else if (claim.sourceEntityId !== address) {
            collisions.push({
               first: claim.sourceName,
               second: persistSource.name,
               table: instruction.physicalTableName,
            });
         }
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
               `'${c.first}' and '${c.second}' compile to different SQL but ` +
               `both materialize into table '${c.table}'`,
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
   compiled: CompiledBuildPlan,
): void {
   if (instruction.destination && getPersistStorageMode() === "off") {
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
   // and not others; failing the run would take down unrelated colocated
   // sources too.
   const declared = declaredStorage(persistSource);
   if (declared && !instruction.destination) {
      // A rollup's source name and `#@ persist storage=` line are
      // synthesized, so it is named by its base and grain, which the author
      // wrote. The grain renders as a set: the wire plan sorts and dedupes
      // dimensions, so quoting it would misrepresent the author's order.
      const rollup = compiled.preaggregatePlans?.[persistSource.sourceID];
      throw new BadRequestError(
         (rollup
            ? `Measures of \`${rollup.baseSourceName}\` ` +
              `pre-aggregated at grain ` +
              `(${rollup.grainDimensions.join(", ")}) declare ` +
              `\`storage=${declared}\``
            : `Source '${persistSource.name}' declares ` +
              `\`#@ persist storage=${declared}\``) +
            `, but the build was instructed with no storage ` +
            `destination, so there is nowhere it may be written. ` +
            `Refusing rather than building the table into the source ` +
            `warehouse, which the annotation asked it not to be ` +
            `written to.`,
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
      reason = redactPgSecrets(errMessage(buildErr));
   }
   // A build that lost a source does not throw, so without this log the
   // failure is invisible in the server's own output.
   logger.warn("Source failed to materialize", {
      packageName: run.owner?.packageName,
      sourceName: persistSource.name,
      physicalTableName: instruction.physicalTableName,
      reason,
   });
   run.failedReasons.push(`${persistSource.name}: ${reason}`);
   run.failures[sourceEntityId] = {
      sourceEntityId,
      sourceName: persistSource.name,
      materializedTableId: instruction.materializedTableId,
      physicalTableName: instruction.physicalTableName,
      reason,
      // Lets a consumer tell a colocated failure from a `storage=` one; it
      // keys by `storageDestinationName ?? connectionName`, like a
      // successful ManifestEntry.
      connectionName: persistSource.connectionName,
      ...(instruction.destination
         ? { storageDestinationName: instruction.destination }
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
