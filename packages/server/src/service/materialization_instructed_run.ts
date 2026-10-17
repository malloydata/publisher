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
   iterGraphSources,
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
   // What this run has already written, keyed by the physical table rather than
   // by the content address: the address says what a table CONTAINS, the
   // coordinate says which table it IS, and only the second answers "have I
   // written this already". Both halves of the guard below need that
   // distinction, in opposite directions.
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
   // Index instructions by sourceID (the stable per-source handle) so the
   // build no longer recomputes the sourceEntityId to find an instruction.
   // Recomputing it here forced a caller's sourceEntityId to equal the publisher's
   // content hash, so a caller that derives sourceEntityIds by any other scheme
   // would have its sources silently skipped (the recomputed sourceEntityId would
   // not match the instruction). sourceEntityId is treated as opaque, caller-assigned
   // identity. A sourceEntityId index is kept as a fallback for instructions without
   // a sourceID (e.g. standalone auto-run).
   const bySourceID = new Map<string, BuildInstruction>();
   const bySourceEntityId = new Map<string, BuildInstruction>();
   for (const instruction of instructions) {
      if (instruction.sourceID) {
         bySourceID.set(instruction.sourceID, instruction);
      }
      // One address, several instructions naming DIFFERENT tables. A host that
      // mints a physical name per source produces this for an ordinary package:
      // several sources share one artifact (`#@ persist` is inherited and
      // `extend` does not change materialization SQL), so a per-source name is a
      // second table for content that already has one.
      //
      // What happens next depends on whether the instructions carry a
      // `sourceID`, which is optional on the wire:
      //  - WITH one, each source finds its own instruction, so every named table
      //    is built, the last to finish is the one `entries` records, and the
      //    rest are left unreferenced — a manifest-driven reclaim never names
      //    them.
      //  - WITHOUT one, this index is the only way back to an instruction and it
      //    holds one per address, so the LAST instruction wins and the earlier
      //    names are never built at all. Nothing can attribute them: an address
      //    is all the publisher has, and it maps to every one of them equally.
      //
      // Reported, not resolved, in both shapes. Picking one would leave the host
      // with an anchor for a table the build declined to write, which is a worse
      // failure than the wasted one and is invisible from the host's side. The
      // fix belongs where the names are minted; this makes the condition visible
      // until then.
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
   // Prefer sourceID matching (so the caller's sourceEntityId scheme
   // stays opaque to the build); the sourceEntityId lookup below is the
   // fallback for instructions without a sourceID (auto-run, or an
   // orchestrated instruction that omits the optional `sourceID`).
   const orchestratedInstruction = index.bySourceID.get(persistSource.sourceID);
   if (orchestratedInstruction) {
      return { instruction: orchestratedInstruction, orchestrated: true };
   }

   // Resolve which instruction (if any) applies to this source
   // BEFORE any eligibility assert runs. A source with NO
   // instruction — a refused sibling the caller never asked to
   // build, alongside others this run does build — must be
   // skipped here rather than examined: the asserts below throw a
   // 422, and unconditionally running them on every persist
   // source in the graph (regardless of whether it had a build to
   // do at all) meant one refused, uninstructed sibling aborted
   // the whole run instead of just the sources actually
   // instructed.
   //
   // computeSourceEntityId calls getSQL(), which throws
   // opaquely for a free-parameter or given source —
   // precisely the sources a caller can never legitimately
   // hold a sourceEntityId for in the first place (the wire
   // build plan omits such a source's entry entirely), so
   // they can never match here regardless. Treat that throw
   // the same as "no match" rather than letting it escape and
   // abort every other instructed source.
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
 * written, rather than when the loop reaches the second one.
 *
 * A mid-loop throw leaves the pair's FIRST table already replaced, and
 * nothing puts it back: the failure path reclaims storage tables only, so a
 * colocated collision strands a half-written table in the customer's own
 * warehouse, and even a reclaimed one cannot restore the rows it
 * overwrote. The publish gate refuses this before any CTAS runs; a rebuild
 * has to match it.
 *
 * It cannot be reasoned away as host-only, either. Collisions are ALWAYS
 * warn-only at load, whatever PERSIST_COLLISION_ENFORCE says (see the note
 * in loadPackage), so a package that was published before the flag went on
 * stays loaded and reaches this build with a MODEL-declared collision
 * intact.
 */
export function assertNoTableCollisions(
   graphs: CompiledBuildPlan["graphs"],
   sources: CompiledBuildPlan["sources"],
   connectionDigests: CompiledBuildPlan["connectionDigests"],
   index: InstructionIndex,
): void {
   const claimedBy = new Map<
      string,
      { sourceName: string; sourceEntityId: string }
   >();
   const collisions: { first: string; second: string; table: string }[] = [];
   for (const graph of graphs) {
      for (const persistSource of iterGraphSources(graph, sources)) {
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
         // Same key as the write guard below, and it has to be: a pre-pass that
         // grouped by a different coordinate would report a collision the guard does
         // not dedupe, or miss one it does.
         const target = physicalTargetKey(instruction, graph.connectionName);
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

   // Inside the per-source try, deliberately: a host that resolved
   // no destination for ONE source has partly-resolved its
   // instruction list, and the reasons named below — the org not
   // enabled, the field omitted — are exactly the ones that hit some
   // sources and not others. Failing the whole run would take down
   // the package's colocated persist sources too, which have no
   // destination to resolve and nothing to do with the refusal. The
   // catch below records it as this source's own build failure, per
   // "one source failing does not end the build".
   //
   // The mirror case, and the one that actually reaches a warehouse
   // write: the SOURCE declares `storage=` and the instruction carries
   // NO destination.
   //
   // Every guard above tests `instruction.destination`, which is what
   // the caller asked for. None tests what the source DECLARED, so a
   // host that resolved no destination — because the org is not
   // enabled for one, because resolution failed, because it simply
   // omitted the field — leaves `isStorageBuild` false in
   // buildOneSource and the source is CTAS'd into its own warehouse.
   //
   // Declaring `storage=` is a statement about where this data may be
   // written, and it is the only one the author made. It is not
   // consent to a warehouse write, and in production the read-only
   // credential this server is given usually cannot perform one
   // anyway, so the observable outcome is a permission error whose
   // cause names neither the destination nor the annotation. Refusing
   // is both the author's instruction and the better diagnosis.
   //
   // Deliberately independent of the mode: "no destination could be
   // resolved" is the same fact whether the tier is off, the org is
   // not enabled, or the host had some other reason. There is no
   // reading of `storage=` under which the source warehouse is the
   // right answer.
   const declared = declaredStorage(persistSource);
   if (declared && !instruction.destination) {
      // The grain renders as a SET rather than quoted text: the wire
      // plan carries only canonically sorted, de-duplicated dimensions,
      // so quoting would misrepresent any grain the author did not
      // write alphabetically.
      //
      // A ROLLUP names neither of the things this message otherwise
      // reaches for: its source name is synthesized and its
      // `#@ persist storage=` line was written by the publisher, so
      // quoting both would send an author looking for a line nobody
      // typed. Named by the base and grain instead, which are what
      // they wrote.
      //
      // Worth branching even though a host that always resolves a
      // destination makes this unreachable. This message is only ever
      // READ when that invariant does not hold, so writing it as
      // though the invariant were reliable is writing it for the one
      // case it cannot occur in. This branch has twice been the shape
      // of a real defect here — an enforcement held by an accident of
      // an import boundary, and an eligibility filter held by an
      // accident of what a walk collected.
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
   // One source failing does not end the build: the sources that
   // did materialize stay usable, and this one records the reason
   // it gave so a consumer can attribute the failure to the unit it
   // belongs to rather than to the whole command. A build that
   // loses every source still fails, below.
   //
   // Redacted against this source's connection CONFIG, not the
   // live connection: a warehouse error can echo the credentials it
   // was handed, and this value is persisted. The config is where
   // the declared secrets are, and it is what every other redaction
   // site passes. Walking a live connection instead is both
   // unnecessary and unsound -- an enumerable accessor that throws
   // once its resource is gone (the state a build failure runs in)
   // escapes the walk and fails the whole run, taking the sources
   // that did materialize with it, and state held in a Map is not
   // walked at all, so a secret there passes through in the clear.
   //
   // A connection missing from the environment is not a reason to
   // leak: getApiConnection throws, so fall back to the
   // connection-free string redactor that redactConnectionSecrets
   // applies anyway.
   let reason: string;
   try {
      reason = redactConnectionSecrets(
         errMessage(buildErr),
         run.environment.getApiConnection(persistSource.connectionName),
      );
   } catch {
      reason = redactPgSecrets(errMessage(buildErr));
   }
   // The manifest carries this reason to the control plane, but a
   // build that lost a source no longer throws -- so without a log
   // here the failure is invisible to anyone reading the server's
   // own output.
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
      // The destination discriminator: without it, a consumer
      // holding only this failure cannot tell a colocated
      // failure from a `storage=` one and, computing
      // `storageDestinationName ?? connectionName` the same
      // way a successful build's ManifestEntry does, would
      // resolve the wrong key.
      connectionName: persistSource.connectionName,
      ...(instruction.destination
         ? { storageDestinationName: instruction.destination }
         : {}),
   };
   // Mirrored into `entries` for the deprecation window, because
   // consumers built against 0.0.245-0.0.246 read the failure from
   // `ManifestEntry.error` and would otherwise see this source as
   // merely ABSENT -- which reads as "nothing to report" rather
   // than "this failed", the fail-dangerous direction. Written
   // unconditionally rather than merged onto whatever is already
   // here: a source can be BOTH seeded (from a reference manifest
   // or bound manifest, which apply no exclusion keyed on this
   // run's content address) and instructed-and-failed, and the
   // seed names the PRIOR generation's table -- a real table,
   // which a serve binding would resolve and serve as fresh.
   // Overwriting is what makes `error` present on the entry any
   // such consumer reads. Remove this whole block with the field.
   run.entries[sourceEntityId] = {
      sourceEntityId,
      sourceName: persistSource.name,
      physicalTableName: instruction.physicalTableName,
      materializedTableId: instruction.materializedTableId,
      error: reason,
   } as ManifestEntry;
}
