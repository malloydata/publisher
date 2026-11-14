// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { getPersistStorageMode } from "../../../config";
import {
   BadRequestError,
   MaterializationEligibilityError,
} from "../../../errors";
import { logger } from "../../../logger";
import { errMessage } from "../../../utils";
import {
   tableIdentity,
   versionedTableName,
   type WriteTarget,
} from "../../versions/materialization_scope";
import {
   CompiledBuildPlan,
   computeSourceEntityId,
   sourcesInBuildOrder,
   tierGateInputs,
   storedBaseOfRollup,
} from "../plan/build_plan";
import { assertTierEligible } from "../plan/materialization_eligibility";
import {
   BuildInstruction,
   BuildPlan,
   LedgerEntry,
   ManifestEntry,
   RefusedSource,
} from "../types";
import {
   sourceIncrementalRefresh,
   type IncrementalRunContext,
} from "./incremental_build";
import type { EligibilityTier } from "./materialization_instructed_run";
import {
   allSourcesRefusedError,
   declaredStorage,
   refusalRecord,
   resolveStorageDestination,
   selfAssignTableName,
   STAGING_ID_LEN,
   tableKeyOf,
} from "./materialization_run_support";
import { hasAuthorTableName } from "./table_ownership";

/**
 * The suffixes auto-run's self-assigned table names take (see
 * deriveSelfInstructions). Neither: the names as written, as only a test
 * seam asks for.
 */
interface SelfTableNaming {
   /**
    * A `scope: version` version's own (versionTableSuffix): on every name,
    * `name=` included.
    */
   versionSuffix?: string;
   /**
    * The package's own (packageTableSuffix): on every name the author did
    * not choose. A `name=` keeps its name and is checked for ownership
    * instead (table_ownership.ts).
    */
   packageSuffix?: string;
}

/*
 * A run's build instructions: derived by auto-run from the compiled plan, or
 * checked when a caller supplies them, and where each instructed table lives.
 */

/**
 * Derive auto-run's own build instructions. Each persist source (respecting
 * `sourceNames`) gets a self-assigned physical name and COPY realization,
 * unless its sourceEntityId is unchanged since `priorEntries`, in which case
 * it is carried forward.
 *
 * An INCREMENTAL source is exempt from carry-forward: unchanged SQL says
 * nothing about whether its DATA moved, so carrying it would never reach its
 * delta. The ledger decides instead, and "nothing new" is a cheap skip.
 *
 * A source the eligibility gate refuses is returned in `refused`. It throws
 * instead when `sourceNames` targets it, or when every authored source the
 * run targets was refused. A refused rollup never causes the throw.
 *
 * `cleared` names the tier each source passed the gate for, so the build loop
 * consults that verdict rather than walking the source again.
 */
export function deriveSelfInstructions(
   compiled: CompiledBuildPlan,
   sourceNames: string[] | undefined,
   priorEntries: Record<string, ManifestEntry>,
   incremental?: IncrementalRunContext,
   naming: SelfTableNaming = {},
): {
   instructions: BuildInstruction[];
   carried: Record<string, ManifestEntry>;
   refused: Record<string, RefusedSource>;
   cleared: Map<string, EligibilityTier>;
   /**
    * Carried tables at a `name=` the author chose, by content address, with
    * the instruction that rebuilds each: carried only while the table is
    * still marked as this package's (see uncarryUnownedTables).
    */
   recheck: Record<string, BuildInstruction>;
} {
   const include = sourceNames ? new Set(sourceNames) : null;
   const instructions: BuildInstruction[] = [];
   const carried: Record<string, ManifestEntry> = {};
   const recheck: Record<string, BuildInstruction> = {};
   const refused: Record<string, RefusedSource> = {};
   const refusals: MaterializationEligibilityError[] = [];
   const cleared = new Map<string, EligibilityTier>();
   const seen = new Set<string>();

   for (const build of compiled.builds) {
      for (const persistSource of sourcesInBuildOrder(
         build,
         compiled.sources,
      )) {
         if (include && !include.has(persistSource.name)) continue;

         // A source that DECLARES `storage=` must never silently downgrade
         // to a colocated CTAS into its own warehouse when
         // PERSIST_STORAGE_MODE=off: the author did not ask for that, and
         // production grants read-only warehouse access. It serves LIVE and
         // Package.storageWarnings reports it. A plain `#@ persist` is
         // unaffected; colocated is its intent.
         if (
            getPersistStorageMode() === "off" &&
            declaredStorage(persistSource)
         ) {
            continue;
         }

         // Never buildable today (#6293); the package warning says so.
         if (storedBaseOfRollup(compiled, persistSource.sourceID)) continue;

         const destination = resolveStorageDestination(persistSource);
         const isRollup =
            !!compiled.preaggregatePlans?.[persistSource.sourceID];
         // Gate BEFORE computeSourceEntityId: an unbound parameter or a given
         // makes its getSQL() throw opaquely.
         //
         // A refused source is SKIPPED and recorded, not thrown for, so one
         // ineligible source does not stop the rest of the package refreshing.
         // The run throws only when nothing else is left to build, or when
         // `sourceNames` asked for exactly this source.
         // A colocated `#@ persist` is just as frozen as a storage build, so
         // an authorize-gated source materialized there would be served to
         // every caller.
         try {
            assertTierEligible(
               persistSource,
               !!destination,
               tierGateInputs(compiled),
            );
         } catch (err) {
            if (!(err instanceof MaterializationEligibilityError)) throw err;
            if (include) throw err;
            if (!refused[persistSource.sourceID]) {
               refused[persistSource.sourceID] = refusalRecord(
                  persistSource,
                  err,
                  isRollup
                     ? "preaggregate"
                     : destination
                       ? "storage"
                       : "colocated",
                  compiled.sourceModelPaths?.[persistSource.sourceID],
               );
               // A refused rollup never fails the run: the author asked for
               // nothing they can see.
               if (!isRollup) refusals.push(err);
               logger.warn(
                  "Skipping a persist source the eligibility gate refuses",
                  {
                     sourceName: persistSource.name,
                     reason: errMessage(err),
                  },
               );
            }
            continue;
         }
         cleared.set(
            persistSource.sourceID,
            destination ? "storage" : "colocated",
         );

         const sourceEntityId = computeSourceEntityId(
            persistSource,
            compiled.connectionDigests,
         );
         if (seen.has(sourceEntityId)) continue;
         seen.add(sourceEntityId);

         // Self-assign the name from `name=` (or the source name) for both
         // destinations. No generational decoration is needed: a storage
         // build replaces the table atomically (`CREATE OR REPLACE`, or the
         // partition create/alter/insert trio in ONE transaction, so a
         // rebuild never empties the table it serves). Orchestrated builds
         // use the host-supplied `physicalTableName`. A `scope: version`
         // run's name gains its version's suffix; any other name the author
         // did not choose gains the package's, so two packages on one schema
         // never build one table (packageTableSuffix).
         const baseName = selfAssignTableName(persistSource);
         const logicalName = naming.versionSuffix
            ? versionedTableName(baseName, naming.versionSuffix)
            : naming.packageSuffix &&
                !hasAuthorTableName(compiled, persistSource)
              ? versionedTableName(baseName, naming.packageSuffix)
              : baseName;

         // The SAME predicate the build's dispatch uses, so the two never
         // disagree about which sources are incremental. A declaration the
         // delta path cannot act on (unsupported dialect, unusable watermark)
         // keeps skip-if-unchanged rather than rebuilding fully every run.
         const deltaEligible =
            sourceIncrementalRefresh(incremental, persistSource, {
               physicalTableName: logicalName,
               storageDestinationName: destination,
               sourceEntityId,
            }) !== undefined;

         const instruction: BuildInstruction = {
            sourceEntityId,
            materializedTableId: `local-${sourceEntityId.substring(
               0,
               STAGING_ID_LEN,
            )}`,
            physicalTableName: logicalName,
            realization: "COPY",
            ...(destination ? { destination } : {}),
         };
         const prior = priorEntries[sourceEntityId];
         // Carry forward only within the SAME destination: sourceEntityId
         // does not encode it, so a source that changes `storage=` must
         // rebuild rather than reuse a table its serve cannot resolve.
         // Never the unsuffixed table a run before package-owned names
         // built: another package on the same schema may have written it
         // since, so the source is rebuilt under its own name.
         if (
            !deltaEligible &&
            prior &&
            prior.physicalTableName &&
            (prior.storageDestinationName ?? undefined) === destination &&
            !(
               logicalName !== baseName &&
               tableIdentity(prior.physicalTableName) ===
                  tableIdentity(baseName)
            )
         ) {
            carried[sourceEntityId] = prior;
            // A table at the author's `name=` may have been written by
            // another package since (or before the mark existed).
            if (
               naming.packageSuffix &&
               logicalName === baseName &&
               prior.physicalTableName === logicalName &&
               hasAuthorTableName(compiled, persistSource)
            ) {
               recheck[sourceEntityId] = instruction;
            }
            continue;
         }

         instructions.push(instruction);
      }
   }

   if (
      refusals.length > 0 &&
      instructions.length === 0 &&
      Object.keys(carried).length === 0
   ) {
      throw allSourcesRefusedError(refusals);
   }

   return { instructions, carried, refused, cleared, recheck };
}

/**
 * Validate caller-supplied build instructions against the compiled build
 * plan: every instructed sourceEntityId must be planned, and only COPY is
 * supported. Throws when the package declares no persist source.
 *
 * Each caller-supplied ledger entry must name a table this run instructs,
 * name it once, belong to a source declaring incremental refresh, and carry
 * the plan's content address for it. An invalid entry is a 400 here, where
 * the caller can react, rather than a failed background run; the address
 * check fires routinely after a publish or rollback changes a source. An
 * entry for a source being reseeded is not validated: `reseed` is the
 * documented recovery from this 400.
 */
export function validateInstructions(
   plan: BuildPlan | null,
   instructions: BuildInstruction[],
   ledger?: LedgerEntry[],
   runReseed = false,
): void {
   if (!plan) {
      throw new BadRequestError(
         "Package has no persist sources; buildInstructions cannot be applied",
      );
   }
   const plannedSourceEntityIds = new Set<string>();
   for (const source of Object.values(plan.sources)) {
      plannedSourceEntityIds.add(source.sourceEntityId);
   }

   for (const instruction of instructions) {
      if (!plannedSourceEntityIds.has(instruction.sourceEntityId)) {
         throw new BadRequestError(
            `Instruction references unknown sourceEntityId '${instruction.sourceEntityId}'`,
         );
      }
      // COPY-only for now; SNAPSHOT lands once clone semantics are defined.
      if (instruction.realization === "SNAPSHOT") {
         throw new BadRequestError(
            "realization=SNAPSHOT is not supported (COPY only)",
         );
      }
   }

   if (ledger === undefined || ledger.length === 0) return;

   // Instructed tables, keyed like a ledger entry, with the plan's facts
   // about each writing source. Resolved by sourceID, else content address
   // (the build loop's order).
   const instructed = new Map<
      string,
      { sourceEntityId: string; refresh: string | null; reseed: boolean }
   >();
   // The same tables keyed WITHOUT their destination, so a destination-less
   // entry can be recognized as stale.
   const instructedIntoStorage = new Set<string>();
   const byAddress = new Map(
      Object.values(plan.sources).map((s) => [s.sourceEntityId, s]),
   );
   for (const instruction of instructions) {
      const planSource =
         (instruction.sourceID
            ? plan.sources[instruction.sourceID]
            : undefined) ?? byAddress.get(instruction.sourceEntityId);
      if (!planSource) continue; // Unreachable: every instruction matched above.
      instructed.set(
         tableKeyOf(
            planSource.connectionName,
            instruction.destination,
            instruction.physicalTableName,
         ),
         {
            sourceEntityId: planSource.sourceEntityId,
            refresh: planSource.refresh ?? null,
            reseed: runReseed || instruction.reseed === true,
         },
      );
      if (instruction.destination) {
         instructedIntoStorage.add(
            tableKeyOf(
               planSource.connectionName,
               undefined,
               instruction.physicalTableName,
            ),
         );
      }
   }

   const seen = new Set<string>();
   for (const entry of ledger) {
      const key = tableKeyOf(
         entry.connectionName,
         entry.storageDestinationName,
         entry.physicalTableName,
      );
      const table =
         `'${entry.physicalTableName}' ` +
         (entry.storageDestinationName
            ? `in storage destination '${entry.storageDestinationName}'`
            : `on connection '${entry.connectionName}'`);
      if (seen.has(key)) {
         throw new BadRequestError(
            `Ledger entry for table ${table} appears more than once`,
         );
      }
      seen.add(key);
      const target = instructed.get(key);
      if (!target) {
         // An entry without `storageDestinationName` for a table this run
         // builds into storage is STALE, not wrong: the source seeds (the
         // build's index keys on the destination, so it finds no boundary).
         // An entry naming a DIFFERENT destination is refused below.
         const storedHere = instructedIntoStorage.has(
            tableKeyOf(
               entry.connectionName,
               undefined,
               entry.physicalTableName,
            ),
         );
         if (entry.storageDestinationName === undefined && storedHere) {
            continue;
         }
         throw new BadRequestError(
            `Ledger entry names table ${table}, which this run's ` +
               `instructions do not build`,
         );
      }
      if (target.reseed) continue;
      if (target.refresh !== "incremental") {
         throw new BadRequestError(
            `Ledger entry names table ${table}, whose source does not ` +
               `declare refresh="incremental"`,
         );
      }
      if (entry.sourceEntityId !== target.sourceEntityId) {
         throw new BadRequestError(
            `Ledger entry for table ${table} was measured under a ` +
               `different definition of its source (its sourceEntityId does ` +
               `not match the plan's) — expected after a publish or ` +
               `rollback that changed the source. Delete the entry to ` +
               `rebuild the source, or set reseed.`,
         );
      }
   }
}

/**
 * Where each instructed table lives, as {@link WRITES_TARGETS_KEY} records it:
 * the instruction's table and destination, with the connection of the plan
 * source it builds. Matched by sourceID when the instruction carries one, else
 * by content address, the order the build matches instructions in. An
 * instruction the plan does not know is left out.
 */
export function writeTargetsFor(
   plan: BuildPlan | null,
   instructions: BuildInstruction[],
): WriteTarget[] {
   if (!plan) return [];
   const byAddress = new Map(
      Object.values(plan.sources).map((s) => [s.sourceEntityId, s]),
   );
   const targets: WriteTarget[] = [];
   for (const instruction of instructions) {
      const source =
         (instruction.sourceID
            ? plan.sources[instruction.sourceID]
            : undefined) ?? byAddress.get(instruction.sourceEntityId);
      if (!source?.connectionName) continue;
      targets.push({
         sourceEntityId: instruction.sourceEntityId,
         physicalTableName: instruction.physicalTableName,
         connectionName: source.connectionName,
         ...(instruction.destination
            ? { storageDestinationName: instruction.destination }
            : {}),
      });
   }
   return targets;
}
