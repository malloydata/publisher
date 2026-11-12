// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   Connection as MalloyConnection,
   PersistSource,
} from "@malloydata/malloy";
import { BuildRunFailedError, combineRetryable } from "./build_failure_class";
import { Manifest } from "@malloydata/malloy";
import {
   BadRequestError,
   MaterializationEligibilityError,
} from "../../../errors";
import { logger } from "../../../logger";
import {
   recordDuplicateTargetSkipped,
   recordManifestBindDegraded,
} from "../../../materialization_metrics";
import { getPersistStorageMode } from "../../../config";
import { withSpan } from "../../../telemetry";
import { errMessage } from "../../../utils";
import { quoteManifestTablePath } from "../../quoting";
import {
   CompiledBuildPlan,
   computeSourceEntityId,
   deriveAnnotationFields,
   sourcesInBuildOrder,
   tierGateInputs,
   storedBaseOfRollup,
} from "../plan/build_plan";
import {
   assertMaterializationEligible,
   assertTierEligible,
} from "../plan/materialization_eligibility";
import {
   BuildInstruction,
   ManifestEntry,
   RefusedSource,
   SourceFailure,
} from "../types";
import type { IncrementalRunContext } from "./incremental_build";
import {
   assertInstructedDestinationWritable,
   assertNoTableCollisions,
   indexInstructions,
   recordInstructedBuildFailure,
   resolveInstruction,
   type EligibilityTier,
   type InstructedRun,
   type ResolvedInstruction,
} from "./materialization_instructed_run";
import {
   allSourcesRefusedError,
   BuildEnvironment,
   BuildQueryMetadata,
   chainedPlanFacts,
   isReclaimableStorageTable,
   physicalTargetKey,
   reportRetainedStorageTables,
} from "./materialization_run_support";
import type { SourceBuilder } from "./source_build";

/** sourceID -> the sourceIDs it reads, merged across every connection's build. */
function upstreamsBySourceID(
   builds: CompiledBuildPlan["builds"],
): Map<string, string[]> {
   const out = new Map<string, string[]>();
   for (const build of builds) {
      for (const [sourceID, reads] of Object.entries(build.dependsOn ?? {})) {
         out.set(sourceID, [...(out.get(sourceID) ?? []), ...reads]);
      }
   }
   return out;
}

/**
 * The instructed tier: a run's build loop, shared by auto-run and orchestrated
 * builds. It walks the instructions in dependency order, gates each source,
 * hands it to its tier (source_build.ts) and records what came of it on the
 * run; a run that commits nothing gives back the storage tables it wrote.
 */
export class InstructedBuild {
   constructor(
      private readonly seams: {
         /**
          * One source's build. The facade routes this, and the failed-run
          * reclaim, back through its own methods of the same names so its
          * specs can drive and substitute them.
          */
         buildOneSource: SourceBuilder["build"];
         reclaimStorageTablesFromFailedRun: (
            builtThisRun: ManifestEntry[],
            environment: BuildEnvironment,
            owner: { environmentId: string; packageName: string },
         ) => Promise<void>;
      },
   ) {}

   /**
    * Quote a seeded upstream's physical path for the in-memory build Manifest,
    * mirroring the CREATE side ({@link quoteManifestTablePath}) so a downstream
    * resolves it on a case-folding engine. Without a resolvable connection the
    * path binds unquoted; on a case-folding engine the downstream CREATE then
    * fails loudly.
    */
   private quoteSeedTablePath(
      sourceEntityId: string,
      physicalTableName: string,
      connectionName: string | undefined,
      connections: Map<string, MalloyConnection>,
   ): string {
      const connection = connectionName
         ? connections.get(connectionName)
         : undefined;
      if (!connection) {
         // Only a named-but-absent connection is a real gap; a missing name is
         // the benign older-caller default.
         if (connectionName) {
            recordManifestBindDegraded();
            logger.warn(
               "Seeded upstream names a connection not present in this build; " +
                  "leaving its manifest path unquoted (a downstream build will " +
                  "fail on a case-folding engine)",
               { sourceEntityId, connectionName },
            );
         }
         return physicalTableName;
      }
      return quoteManifestTablePath(physicalTableName, connection.dialectName);
   }

   /**
    * Shared build loop for both auto-run and orchestrated builds. Seeds the
    * manifest with carried-forward (reused) upstream entries so downstream
    * references resolve, then builds each instructed source in dependency
    * order. Returns the full entry map (carried + freshly built). The package
    * was compiled by the caller; this runs outside the package lock.
    */
   async run(
      compiled: CompiledBuildPlan,
      environment: BuildEnvironment,
      instructions: BuildInstruction[],
      seedEntries: Record<string, ManifestEntry>,
      signal: AbortSignal,
      strict = false,
      // Used only to reclaim this run's storage tables on failure.
      owner?: { environmentId: string; packageName: string },
      buildMetadata?: BuildQueryMetadata,
      // Undefined leaves every source on the full-rebuild path.
      incremental?: IncrementalRunContext,
      // Auto-run refusals, named in the error of a run whose every instructed
      // source failed so it accounts for every unbuilt source.
      priorRefusals: RefusedSource[] = [],
      // Tiers auto-run already cleared (see deriveSelfInstructions); a listed
      // source is not asserted again.
      clearedTiers?: ReadonlyMap<string, EligibilityTier>,
   ): Promise<{
      entries: Record<string, ManifestEntry>;
      failures: Record<string, SourceFailure>;
      /**
       * Instructed sources the eligibility gate refused. Each is also in
       * `failures`; the count is kept apart so a refusal is metered as one.
       */
      sourcesRefused: number;
   }> {
      const { builds, sources, connectionDigests, connections } = compiled;
      const index = indexInstructions(instructions);
      const run: InstructedRun = {
         compiled,
         environment,
         owner,
         buildMetadata,
         incremental,
         clearedTiers,
         index,
         // Once per run: it depends only on the plan.
         planFacts: chainedPlanFacts(sources, connectionDigests),
         ...this.seedInstructedRun(seedEntries, connections, strict),
         builtThisRun: [],
         retainedThisRun: [],
         failures: {},
         failedBySourceID: new Map(),
         upstreamsBySourceID: upstreamsBySourceID(builds),
         failedReasons: [],
         refusals: [],
         builtSources: [],
         writtenTargets: new Map(),
      };
      assertNoTableCollisions(builds, sources, connectionDigests, index);

      try {
         for (const build of builds) {
            const connection = connections.get(build.connectionName);
            if (!connection) {
               throw new BadRequestError(
                  `Connection '${build.connectionName}' not found`,
               );
            }
            for (const persistSource of sourcesInBuildOrder(build, sources)) {
               if (signal.aborted) throw new Error("Build cancelled");
               await withSpan(
                  "publisher.materialization.build_source",
                  // Nested under the run's span, which names the package.
                  { "malloy.source": persistSource.name },
                  () =>
                     this.buildInstructedSource(
                        run,
                        persistSource,
                        build.connectionName,
                        connection,
                     ),
               );
            }
         }

         // A run that produced nothing must not report success with errors
         // attached. Thrown inside the try so it takes the same cleanup path as
         // any other total failure.
         if (run.builtSources.length === 0 && run.failedReasons.length > 0) {
            throw new BuildRunFailedError(
               [
                  ...run.failedReasons,
                  ...run.refusals.map((e) => e.message),
                  ...priorRefusals.map((r) => r.message),
               ].join("; "),
               // Every source's own answer (a refusal is recorded in
               // `failures` as `false` too), plus auto-run's refusals, which
               // never reached this loop and are deterministic.
               combineRetryable([
                  ...Object.values(run.failures).map((f) => f.retryable),
                  ...priorRefusals.map(() => false),
               ]),
            );
         }
         // Every instructed source was refused: the refusal is the answer.
         if (run.builtSources.length === 0 && run.refusals.length > 0) {
            throw allSourcesRefusedError(run.refusals);
         }
      } catch (err) {
         // A run that commits no manifest leaves its written tables unreachable
         // to manifest-driven GC, so reclaim them before rethrowing.
         // Best-effort: the build's own failure is what the caller needs.
         if (owner) {
            reportRetainedStorageTables(run.retainedThisRun, owner.packageName);
            await this.seams.reclaimStorageTablesFromFailedRun(
               run.builtThisRun,
               environment,
               owner,
            );
         }
         throw err;
      }

      return {
         entries: run.entries,
         failures: run.failures,
         sourcesRefused: run.refusals.length,
      };
   }

   /**
    * The build Manifest and entry map a run starts from: every carried-forward
    * entry, so references to an upstream this run does not rebuild resolve.
    */
   private seedInstructedRun(
      seedEntries: Record<string, ManifestEntry>,
      connections: CompiledBuildPlan["connections"],
      strict: boolean,
   ): Pick<InstructedRun, "manifest" | "entries"> {
      // Accumulates physical names as sources build so downstreams resolve to
      // them. In strict mode an upstream neither built nor seeded fails the
      // compile (runtime-manifest-strict-miss) instead of recomputing live.
      const manifest = new Manifest();
      manifest.strict = strict;
      const entries: Record<string, ManifestEntry> = {};
      for (const [sourceEntityId, entry] of Object.entries(seedEntries)) {
         if (entry.physicalTableName) {
            // The build Manifest feeds a downstream `FROM` verbatim, so it needs
            // the quoting the table was CREATEd with. `entries` (the committed
            // manifest) keeps the logical, unquoted name.
            manifest.update(sourceEntityId, {
               tableName: this.quoteSeedTablePath(
                  sourceEntityId,
                  entry.physicalTableName,
                  entry.connectionName,
                  connections,
               ),
            });
         }
         entries[sourceEntityId] = entry;
      }
      return { manifest, entries };
   }

   /**
    * One persist source's turn in the build loop: resolve its instruction, gate
    * it, and build it — recording the outcome on `run`. A source that fails or is
    * refused is recorded and returns normally; only an orchestration failure
    * throws.
    */
   private async buildInstructedSource(
      run: InstructedRun,
      persistSource: PersistSource,
      graphConnectionName: string,
      connection: MalloyConnection,
   ): Promise<void> {
      const { compiled } = run;
      const resolved = resolveInstruction(
         run.index,
         persistSource,
         compiled.connectionDigests,
      );
      if (!resolved) return;
      const { instruction, orchestrated } = resolved;

      if (this.refuseIneligibleInstructedSource(run, persistSource, resolved)) {
         return;
      }

      // The manifest is keyed by the CONTENT sourceEntityId (what Malloy
      // recomputes to resolve upstream references), not the instruction's
      // identity. A sourceID match has not computed it yet.
      const sourceEntityId =
         resolved.sourceEntityId ??
         computeSourceEntityId(
            persistSource,
            compiled.connectionDigests,
            compiled.targets?.[persistSource.sourceID],
         );

      // Re-assert (idempotent) so no path into a storage build is ungated.
      if (
         !orchestrated &&
         instruction.destination &&
         getPersistStorageMode() !== "off" &&
         run.clearedTiers?.get(persistSource.sourceID) !== "storage"
      ) {
         assertMaterializationEligible(
            persistSource,
            deriveAnnotationFields(persistSource),
         );
      }

      // One physical table, written once. A base and its extension (`#@
      // persist` is inherited) compile to the same address, and a source used
      // across models appears in several builds. Only the same-content case is
      // decided here; DIFFERENT definitions on one table were reported (and
      // refused under PERSIST_COLLISION_ENFORCE) by assertNoTableCollisions, so
      // reaching here means the deployment chose to warn and write anyway.
      const target = physicalTargetKey(instruction, graphConnectionName);
      const written = run.writtenTargets.get(target);
      if (written && written.sourceEntityId === sourceEntityId) {
         // Another route to the same table: nothing to build or record.
         recordDuplicateTargetSkipped();
         logger.debug("Skipping a source whose table this run built", {
            sourceName: persistSource.name,
            builtAs: written.sourceName,
            physicalTableName: instruction.physicalTableName,
         });
         return;
      }

      let entry;
      try {
         // An instructed destination that cannot be honored REFUSES, never
         // falls through to a colocated build: that would write the caller's
         // name into the source's own warehouse and answer success.
         assertInstructedDestinationWritable(
            persistSource,
            instruction,
            compiled,
         );

         entry = await this.seams.buildOneSource(
            persistSource,
            instruction,
            connection,
            run.manifest,
            run.environment,
            run.entries,
            run.buildMetadata,
            run.incremental,
            // The CONTENT address, distinct from the instruction's
            // caller-assigned identity: the ledger is keyed by it so a
            // boundary can never be read against different SQL.
            sourceEntityId,
            run.planFacts,
         );
         // A manifest travels without its build plan, so this is the only
         // signal that a table belongs to a rollup in no model file; served as
         // an ordinary binding it would be silently dropped (see
         // preaggregation_serve_bindings).
         if (compiled.preaggregatePlans?.[persistSource.sourceID]) {
            entry = { ...entry, origin: "preaggregate" as const };
         }
      } catch (buildErr) {
         recordInstructedBuildFailure(
            run,
            persistSource,
            instruction,
            sourceEntityId,
            buildErr,
         );
         return;
      }
      run.builtSources.push(persistSource.name);
      run.entries[sourceEntityId] = entry;
      run.writtenTargets.set(target, {
         sourceEntityId,
         sourceName: persistSource.name,
      });
      if (isReclaimableStorageTable(entry)) {
         run.builtThisRun.push(entry);
      } else if (entry.storageDestinationName) {
         // Recorded, not logged: only a failed run leaves these behind.
         run.retainedThisRun.push(entry);
      }
   }

   /**
    * Gate one instructed source, recording a refusal as that source's failure.
    * Returns true when the source was refused and must not be built.
    */
   private refuseIneligibleInstructedSource(
      run: InstructedRun,
      persistSource: PersistSource,
      resolved: ResolvedInstruction,
   ): boolean {
      const { compiled } = run;
      const { instruction, orchestrated } = resolved;
      // A refusal fails THIS source, not the run (as in
      // deriveSelfInstructions), and is reported in `failures` because an
      // absent entry reads as "built". A host building from the plan never
      // instructs a refused source, so reaching the catch means the plan and
      // this gate disagreed.
      try {
         // A host instructing from an older plan can still name a rollup over
         // a stored base, which can never build (storedBaseOfRollup). Refuse
         // it with the reason, rather than fail inside the chained build.
         // No `reason` enum value: the refusal rides `failures` as text.
         const storedBase = storedBaseOfRollup(
            compiled,
            persistSource.sourceID,
         );
         if (storedBase) {
            throw new MaterializationEligibilityError({
               message:
                  `'${persistSource.name}' is a '#@ preaggregate storage=' rollup ` +
                  `over '${storedBase.name}', which is itself persisted with ` +
                  `'storage=': a rollup over a stored base cannot build or serve yet`,
            });
         }
         // Host-supplied storage instructions are gated too, not trusted.
         // Skipped when the mode is off: the destination refusal owns that.
         const storage =
            orchestrated &&
            !!instruction.destination &&
            getPersistStorageMode() !== "off";
         // Every other instruction (colocated, or any build while the mode is
         // off) takes the colocated gate however it was instructed, unless
         // auto-run already cleared the source through one of the two gates.
         // Never both: the storage gate already runs the same IR walk.
         if (storage || !run.clearedTiers?.has(persistSource.sourceID)) {
            assertTierEligible(
               persistSource,
               storage,
               tierGateInputs(compiled),
            );
         }
      } catch (err) {
         if (!(err instanceof MaterializationEligibilityError)) throw err;
         // Keyed by the caller's address: a refused source may have no
         // content address (getSQL throws for a given or free parameter).
         const failedKey =
            resolved.sourceEntityId ?? instruction.sourceEntityId;
         if (!run.failures[failedKey]) {
            const reason = errMessage(err);
            run.refusals.push(err);
            run.failedBySourceID.set(persistSource.sourceID, {
               name: persistSource.name,
               reason,
               retryable: false,
            });
            logger.warn("Instructed source refused by the eligibility gate", {
               packageName: run.owner?.packageName,
               sourceName: persistSource.name,
               physicalTableName: instruction.physicalTableName,
               reason,
            });
            run.failures[failedKey] = {
               sourceEntityId: failedKey,
               sourceName: persistSource.name,
               materializedTableId: instruction.materializedTableId,
               physicalTableName: instruction.physicalTableName,
               reason,
               // Marks the failure permanent, so a caller that retries
               // failures can tell it from one that may clear.
               refused: true,
               retryable: false,
               connectionName: persistSource.connectionName,
               ...(instruction.destination
                  ? { storageDestinationName: instruction.destination }
                  : {}),
            };
            // Mirrored into `entries` for the `ManifestEntry.error`
            // deprecation window, as the build-failure path below does.
            run.entries[failedKey] = {
               sourceEntityId: failedKey,
               sourceName: persistSource.name,
               physicalTableName: instruction.physicalTableName,
               materializedTableId: instruction.materializedTableId,
               error: reason,
            } as ManifestEntry;
         }
         return true;
      }
      return false;
   }
}
