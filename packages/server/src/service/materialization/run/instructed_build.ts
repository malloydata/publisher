// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { assertInlinedJoinsUnique } from "./inline_uniqueness";
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
   type ColocatedRefusal,
   type EligibilityTier,
   type InstructedRun,
   type PendingRefusal,
   type ResolvedInstruction,
} from "./materialization_instructed_run";
import {
   allSourcesRefusedError,
   BuildEnvironment,
   BuildQueryMetadata,
   chainedPlanFacts,
   isReclaimableStorageTable,
   physicalTargetKey,
   refusalRecord,
   reportRetainedStorageTables,
   type WarehouseReach,
} from "./materialization_run_support";
import type { Materialization } from "../../../storage/DatabaseInterface";
import {
   nameViewIntentFor,
   nameViewKey,
   ownNameTableIn,
   ownViewAtTable,
   preflightNameView,
   publishNameViews,
   swapDependentsFor,
   tableAtNameIntentFor,
   viewToTableSwap,
   type NameViewContext,
   type NameViewIntent,
   type NameViewRecord,
   type OwnNameTable,
   type SwapDependents,
} from "./name_view";
import { nameViewOwner } from "./name_view_sql";
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
         /**
          * The package's runs, from its own records: what tells a table at a
          * `name=` this package wrote from anyone else's (name_view.ts).
          * Absent: no such table is ever recognized, and every one refuses.
          */
         packageRuns?: (
            environmentName: string,
            packageName: string,
         ) => Promise<Materialization[]>;
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
      // The host instructed this run (orchestrated): a host-supplied upstream
      // entry at the wrong tier is refused, not recomputed (see
      // ChainedPlanFacts.hostInstructed).
      hostInstructed = false,
      // What a strict build does with the warehouse recompute (#6756).
      warehouseReach: WarehouseReach = "recompute",
   ): Promise<{
      entries: Record<string, ManifestEntry>;
      failures: Record<string, SourceFailure>;
      /**
       * Instructed sources the eligibility gate refused, metered as refused.
       * `refusalsInFailures` of them are also in `failures` (no sibling built
       * their address); the rest are in `refusedBesideBuilt`.
       */
      sourcesRefused: number;
      refusalsInFailures: number;
      /**
       * Refused sources whose table an eligible sibling built
       * (ms2data/service#7078), by sourceID: the table stands and serves its
       * eligible sources, and only these answer live.
       */
      refusedBesideBuilt: Record<string, RefusedSource>;
      /**
       * What became of each `name=` view the built sources call for, made
       * once every source built (name_view.ts). Empty without any.
       */
      nameViews: NameViewRecord[];
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
         planFacts: {
            ...chainedPlanFacts(
               sources,
               connectionDigests,
               // A host can imply a destination the annotation does not declare,
               // so the instruction decides a source's current tier (#7087).
               (sourceID, sourceEntityId) =>
                  index.bySourceID.get(sourceID) ??
                  index.bySourceEntityId.get(sourceEntityId),
            ),
            hostInstructed,
            warehouseReach,
         },
         ...this.seedInstructedRun(seedEntries, connections, strict),
         builtThisRun: [],
         retainedThisRun: [],
         failures: {},
         failedBySourceID: new Map(),
         upstreamsBySourceID: upstreamsBySourceID(builds),
         failedReasons: [],
         refusals: [],
         pendingRefusals: [],
         priorRefusedIDs: new Set(priorRefusals.map((r) => r.sourceID)),
         builtAddresses: new Set(),
         builtSources: [],
         writtenTargets: new Map(),
         nameViewIntents: [],
         nameViewOwnTables: new Map(),
         replacedNameViews: [],
      };
      run.ownNameTableAt = this.ownNameTableLookup(run);
      run.otherRunActive = this.otherRunActiveLookup(run);
      assertNoTableCollisions(builds, sources, connectionDigests, index);

      let settled: ReturnType<typeof settleRefusals> = {
         refusalsInFailures: 0,
         refusedBesideBuilt: {},
      };
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

         settled = settleRefusals(run);

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
         // Every instructed source was refused by its own gate: the refusal is
         // the answer. Not when a source its own gate admits was held back only
         // because a colocated sibling on its table is refused: the address
         // fails in `failures`, as an address with a refused source always
         // has, and the run is not failed for it (ms2data/service#7078).
         if (
            run.builtSources.length === 0 &&
            run.refusals.length > 0 &&
            !run.heldBackBySibling
         ) {
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

      // Only once every source built, so a run that fails part-way never
      // points a view at a table its manifest will not name. Never throws.
      const nameViews = [
         ...(run.replacedNameViews ?? []),
         ...(run.nameViewIntents?.length
            ? await publishNameViews(
                 run.nameViewIntents,
                 nameViewContext(run),
                 run.nameViewOwnTables,
              )
            : []),
      ];

      return {
         entries: run.entries,
         failures: run.failures,
         sourcesRefused: run.refusals.length,
         ...settled,
         nameViews,
      };
   }

   /**
    * How the run asks whether this package's own records say it wrote the
    * table at a `name=`: the package's runs, read once, on first ask.
    */
   private ownNameTableLookup(
      run: InstructedRun,
   ): (intent: NameViewIntent) => Promise<OwnNameTable | undefined> {
      const packageRuns = this.seams.packageRuns;
      const environmentName =
         run.buildMetadata?.context.environment ??
         run.environment.getEnvironmentName?.();
      const packageName =
         run.buildMetadata?.context.package ?? run.owner?.packageName;
      let runs: Promise<Materialization[]> | undefined;
      return async (intent) => {
         if (!packageRuns || !environmentName || !packageName) return undefined;
         runs ??= packageRuns(environmentName, packageName);
         return ownNameTableIn(await runs, intent);
      };
   }

   /**
    * Whether a run of this package other than this one is building, read
    * fresh from the package's records: this run is itself active, so more
    * than one active run means another. Unknown counts as yes.
    */
   private otherRunActiveLookup(run: InstructedRun): () => Promise<boolean> {
      const packageRuns = this.seams.packageRuns;
      const environmentName =
         run.buildMetadata?.context.environment ??
         run.environment.getEnvironmentName?.();
      const packageName =
         run.buildMetadata?.context.package ?? run.owner?.packageName;
      return async () => {
         if (!packageRuns || !environmentName || !packageName) return true;
         try {
            const active = (
               await packageRuns(environmentName, packageName)
            ).filter(
               (m) =>
                  m.status === "PENDING" || m.status === "MANIFEST_ROWS_READY",
            );
            return active.length > 1;
         } catch {
            return true;
         }
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

      // A colocated table is served by its address, so the bind refuses the
      // whole address when any source on it is refused
      // (collectColocatedSourceEligibility). Building it would write a table
      // nothing serves; refuse this source with its sibling's reason instead,
      // so every source on the address gets one outcome whatever the order.
      if (!instruction.destination) {
         const sibling = colocatedRefusalOn(
            run,
            sourceEntityId,
            persistSource.sourceID,
         );
         if (sibling) {
            recordPendingRefusal(
               run,
               persistSource,
               instruction,
               sourceEntityId,
               new MaterializationEligibilityError({
                  reason: sibling.reason,
                  message:
                     `'${persistSource.name}' is not built: it shares its ` +
                     `table with '${sibling.name}', which the eligibility ` +
                     `gate refuses for a colocated build, and a colocated ` +
                     `table is served to every source on it. ` +
                     sibling.message,
               }),
               false,
            );
            run.heldBackBySibling = true;
            return;
         }
      }

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
      // refused unless PERSIST_COLLISION_ENFORCE=false) by
      // assertNoTableCollisions, so reaching here means the deployment opted
      // out, to warn and write anyway.
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

      const nameView = nameViewIntentFor(
         compiled,
         persistSource,
         instruction,
         sourceEntityId,
         nameViewOwnerOf(run),
      );
      // A table called `name=` itself, where this package's own view may
      // stand from a build that named its table by generation.
      const tableAtName = nameView
         ? undefined
         : tableAtNameIntentFor(
              compiled,
              persistSource,
              instruction,
              sourceEntityId,
              nameViewOwnerOf(run),
           );
      let replacedView: { target: string } | undefined;
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

         // An inlined join_one must match at most one row (P8): checked on
         // the source's own connection before anything is written.
         await assertInlinedJoinsUnique(
            compiled.materializePlans?.[persistSource.sourceID],
            connection,
         );

         // A `name=` someone else holds fails the source before it writes.
         let swapDependents: SwapDependents | undefined;
         if (nameView) {
            const probed = await preflightNameView(
               nameView,
               nameViewContext(run, connection),
            );
            if (probed?.slot.state === "own_table") {
               run.nameViewOwnTables?.set(
                  nameViewKey(nameView),
                  probed.slot.table,
               );
            } else if (probed && !instruction.destination) {
               swapDependents = swapDependentsFor(
                  nameView,
                  probed.slot,
                  probed.dialect,
               );
            }
         } else if (tableAtName) {
            // This package's own view at the name: the table replaces it.
            const ownView = await ownViewAtTable(
               tableAtName,
               nameViewContext(run, connection),
            );
            if (ownView) {
               swapDependents = viewToTableSwap(
                  tableAtName,
                  ownView.target,
                  ownView.dialect,
               );
               replacedView = ownView;
            }
         }

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
            // Passed only when present, so a seam's recorded call is unchanged.
            ...(swapDependents ? ([swapDependents] as const) : []),
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
      run.builtAddresses.add(sourceEntityId);
      run.writtenTargets.set(target, {
         sourceEntityId,
         sourceName: persistSource.name,
      });
      if (nameView) run.nameViewIntents?.push(nameView);
      if (tableAtName && replacedView) {
         run.replacedNameViews?.push({
            ...tableAtName,
            outcome: "replaced_by_table",
            previousTarget: replacedView.target,
            message:
               `the table replaced this package's view '${tableAtName.viewName}', ` +
               `which read '${replacedView.target}'`,
         });
         logger.info("A built table replaced this package's name= view", {
            sourceName: persistSource.name,
            physicalTableName: instruction.physicalTableName,
            previousTarget: replacedView.target,
         });
      }
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
      // Auto-run refused it and reported it already; it reaches the loop only
      // through an eligible sibling's instruction on the same address.
      if (run.priorRefusedIDs.has(persistSource.sourceID)) return true;
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
         recordPendingRefusal(
            run,
            persistSource,
            instruction,
            // Keyed by the caller's address: a refused source may have no
            // content address (getSQL throws for a given or free parameter).
            failedKey,
            err,
            true,
         );
         return true;
      }
      return false;
   }
}

/** The package a run's `name=` views are marked as made by. */
function nameViewOwnerOf(run: InstructedRun): string {
   return nameViewOwner(
      run.buildMetadata?.context.environment ??
         run.environment.getEnvironmentName?.() ??
         "",
      run.buildMetadata?.context.package ?? run.owner?.packageName ?? "",
   );
}

/**
 * How a run reaches the site of a `name=` view: the source's connection (the
 * one this build holds, when given), or the destination the run builds into.
 */
export function nameViewContext(
   run: Pick<
      InstructedRun,
      "compiled" | "environment" | "ownNameTableAt" | "otherRunActive"
   >,
   connection?: MalloyConnection,
): NameViewContext {
   return {
      ...(run.ownNameTableAt ? { ownTableAt: run.ownNameTableAt } : {}),
      ...(run.otherRunActive ? { otherRunActive: run.otherRunActive } : {}),
      connectionFor: (name) =>
         (connection && connection.name === name ? connection : undefined) ??
         run.compiled.connections.get(name),
      destinationFor: (name) => {
         try {
            return run.environment.getStorageDestination(name);
         } catch {
            return undefined;
         }
      },
      environmentPath: run.environment.getEnvironmentPath(),
   };
}

/**
 * Record the run's gate refusals against their addresses, now that the loop
 * knows which addresses it built (ms2data/service#7078). A base and its
 * `extend` share one content address, and the gate decides per source. A
 * refusal on an address an eligible sibling built fails only the refused
 * source: the table stands, its eligible sources serve it, and the refused
 * one answers live (routing skips a gated entry point). It stays out of
 * `failures` and `entries`, which the control plane reads by address, and is
 * reported per source instead. A refusal on an address nothing built is
 * recorded as before, once per address. Run order no longer decides either.
 */
function settleRefusals(run: InstructedRun): {
   refusalsInFailures: number;
   refusedBesideBuilt: Record<string, RefusedSource>;
} {
   let refusalsInFailures = 0;
   const refusedBesideBuilt: Record<string, RefusedSource> = {};
   const unbuilt = new Map<string, PendingRefusal[]>();
   for (const pending of run.pendingRefusals) {
      if (run.builtAddresses.has(pending.failedKey)) {
         refusedBesideBuilt[pending.refused.sourceID] = pending.refused;
         logger.warn(
            "A refused source shares a table an eligible source built; the table stands and only the refused source answers live",
            {
               packageName: run.owner?.packageName,
               sourceName: pending.failure.sourceName,
               physicalTableName: pending.failure.physicalTableName,
               reason: pending.failure.reason,
            },
         );
         continue;
      }
      const onAddress = unbuilt.get(pending.failedKey);
      if (onAddress) onAddress.push(pending);
      else unbuilt.set(pending.failedKey, [pending]);
   }
   for (const [address, pendings] of unbuilt) {
      // Once per address, as before; a build failure already there stands.
      if (run.failures[address]) continue;
      // The same failure whatever the run order: a source its own gate refused
      // before one refused only for sharing the table, then by sourceID.
      const chosen = [...pendings].sort(
         (a, b) =>
            Number(b.own) - Number(a.own) ||
            (a.sourceID < b.sourceID ? -1 : a.sourceID > b.sourceID ? 1 : 0),
      )[0];
      refusalsInFailures++;
      run.failures[address] = chosen.failure;
      run.entries[address] = chosen.errorEntry;
   }
   run.pendingRefusals = [];
   return { refusalsInFailures, refusedBesideBuilt };
}

/**
 * Record one source's gate refusal on `run`: counted and logged now, held
 * against its address until {@link settleRefusals} knows whether an eligible
 * sibling built that address.
 */
function recordPendingRefusal(
   run: InstructedRun,
   persistSource: PersistSource,
   instruction: BuildInstruction,
   failedKey: string,
   err: MaterializationEligibilityError,
   own: boolean,
): void {
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
   run.pendingRefusals.push({
      failedKey,
      failure: {
         sourceEntityId: failedKey,
         sourceName: persistSource.name,
         materializedTableId: instruction.materializedTableId,
         physicalTableName: instruction.physicalTableName,
         reason,
         // Marks the failure permanent, so a caller that retries failures can
         // tell it from one that may clear.
         refused: true,
         retryable: false,
         connectionName: persistSource.connectionName,
         ...(instruction.destination
            ? { storageDestinationName: instruction.destination }
            : {}),
      },
      // Mirrored into `entries` for the `ManifestEntry.error` deprecation
      // window, as the build-failure path does.
      errorEntry: {
         sourceEntityId: failedKey,
         sourceName: persistSource.name,
         physicalTableName: instruction.physicalTableName,
         materializedTableId: instruction.materializedTableId,
         error: reason,
      } as ManifestEntry,
      refused: refusalRecord(
         persistSource,
         err,
         run.compiled.preaggregatePlans?.[persistSource.sourceID]
            ? "preaggregate"
            : instruction.destination
              ? "storage"
              : "colocated",
         run.compiled.sourceModelPaths?.[persistSource.sourceID],
      ),
      sourceID: persistSource.sourceID,
      own,
   });
}

/**
 * Another planned source on `address` that the colocated gate refuses, or
 * undefined. Only the sources on that address are gated, once per address per
 * run, so an unrelated source can never fail this build. Any error from the
 * gate refuses, as the bind's collectColocatedSourceEligibility treats it, so
 * build and serve agree on the address. The first refused source in sourceID
 * order is named, so the reason never depends on run order.
 */
function colocatedRefusalOn(
   run: InstructedRun,
   address: string,
   sourceID: string,
): ColocatedRefusal | undefined {
   const { compiled } = run;
   if (!run.sourceIDsByAddress) {
      const byAddress = new Map<string, string[]>();
      for (const [id, source] of Object.entries(compiled.sources)) {
         let sourceAddress: string;
         try {
            sourceAddress = computeSourceEntityId(
               source,
               compiled.connectionDigests,
               compiled.targets?.[id],
            );
         } catch {
            // No address, so no table it could share.
            continue;
         }
         const ids = byAddress.get(sourceAddress);
         if (ids) ids.push(id);
         else byAddress.set(sourceAddress, [id]);
      }
      run.sourceIDsByAddress = byAddress;
   }
   run.colocatedRefusalsByAddress ??= new Map();
   let refusals = run.colocatedRefusalsByAddress.get(address);
   if (!refusals) {
      refusals = [];
      const ids = [...(run.sourceIDsByAddress.get(address) ?? [])].sort();
      for (const id of ids) {
         const source = compiled.sources[id];
         if (!source) continue;
         try {
            assertTierEligible(source, false, tierGateInputs(compiled));
         } catch (err) {
            if (!(err instanceof MaterializationEligibilityError)) {
               logger.warn(
                  "The colocated gate could not decide a source; refusing its address, as the bind does",
                  {
                     packageName: run.owner?.packageName,
                     sourceName: source.name,
                     error: errMessage(err),
                  },
               );
            }
            refusals.push({
               sourceID: id,
               name: source.name,
               reason:
                  err instanceof MaterializationEligibilityError
                     ? err.reason
                     : undefined,
               message: errMessage(err),
            });
         }
      }
      run.colocatedRefusalsByAddress.set(address, refusals);
   }
   return refusals.find((refusal) => refusal.sourceID !== sourceID);
}
