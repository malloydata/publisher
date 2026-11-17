// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One instructed run's build loop ({@link InstructedBuild}), shared by the
 * auto-run and orchestrated runs: it walks the instructions in dependency
 * order, gates each source, hands it to its tier through the source build,
 * runs the key checks over each written table, and records each source's
 * outcome on the {@link InstructedRun}. Called by the materialization service;
 * {@link tableSiteContext} is also read by the service and the reclaimer.
 *
 * Effects: warehouse and destination SQL through the tier adapters, the key
 * checks and the table marks; build telemetry and logs. A run that commits
 * nothing gives back the storage tables it wrote. It never commits a run
 * manifest; the committer does.
 */

import { BuildCancelledError } from "../domain/build_cancelled";
import { keyCheckCompilerKey } from "../domain/key_check";
import {
   assertInlinedJoinsUnique,
   pairLeafKeyChecksOf,
   checkDigest,
   tableIdentityKey,
   type StoredJoin,
   type UniquenessLedger,
} from "./inline_uniqueness";
import type {
   OverStoredBase,
   ParentKeyCheck,
   RollupKeyCheck,
} from "../domain/table_plans";
import type { InlineUniquenessCheck } from "../domain/table_plans";
import { buildSQLOf, presentAs } from "../plan/addressing";
import { outputColumnsOf } from "../../governance/ir/compiled_model";
import type {
   Connection as MalloyConnection,
   PersistSource,
} from "@malloydata/malloy";
import {
   BuildRunFailedError,
   combineRetryable,
   withFailedRunManifest,
} from "./build_failure_class";
import { Manifest } from "@malloydata/malloy";
import {
   BadRequestError,
   MaterializationEligibilityError,
} from "../../../errors";
import { logger } from "../../../logger";
import {
   type KeyCheckKind,
   recordDuplicateTargetSkipped,
   recordKeyCheck,
   recordManifestBindDegraded,
} from "../../../materialization_metrics";
import { getPersistStorageMode } from "../../../config";
import { withSpan } from "../../../telemetry";
import { errMessage } from "../../../utils";
import { quoteManifestTablePath } from "../../quoting";
import { ColocatedTier } from "./colocated_build_strategy";
import { StorageBuildStrategy } from "./storage_build_strategy";
import {
   computeSourceEntityId,
   deriveAnnotationFields,
   tierGateInputs,
} from "../plan/build_plan";
import {
   type CompiledBuildPlan,
   materializePlansOf,
   sourcesInBuildOrder,
} from "../domain/compiled_build_plan";
import {
   assertMaterializationEligible,
   assertPlacementNamed,
   assertTierEligible,
} from "../plan/materialization_eligibility";
import {
   BuildInstruction,
   ManifestEntry,
   RefusedSource,
   SourceFailure,
} from "../domain/types";
import { sourceIncrementalRefresh } from "./incremental_build";
import {
   assertInstructedDestinationWritable,
   assertNoTableCollisions,
   indexInstructions,
   recordInstructedBuildFailure,
   resolveInstruction,
   type ColocatedRefusal,
   type InstructedBuildOptions,
   type InstructedRun,
   type PendingRefusal,
   type ResolvedInstruction,
} from "./materialization_instructed_run";
import { freshnessOf } from "../domain/freshness";
import {
   allSourcesRefusedError,
   BuildEnvironment,
   chainedPlanFacts,
   upstreamEntryIsCurrent,
   isReclaimableStorageTable,
   manifestExcludingStorage,
   physicalTargetKey,
   refusalRecord,
   reportRetainedStorageTables,
   selfAssignTableName,
   statementRunOptions,
} from "./materialization_run_support";
import {
   dropOwnNameView,
   provenOwnView,
   replaceOwnViewStatements,
} from "./leftover_name_views";
import { packageOwner } from "./table_mark_sql";
import { atTableSite, type TableSiteContext } from "./table_site";
import {
   LockOrderError,
   withOwnedNameLock,
   assertTableOwnable,
   hasAuthorTableName,
   markOwnedTable,
   packageRecordsName,
   TABLE_OWNED_ELSEWHERE,
   TableOwnershipError,
   type OwnedTableTarget,
} from "./table_ownership";
import type { SourceBuildOptions } from "./source_build";
import type { KeyCheckRecord } from "./tier";
import {
   buildKindOf,
   buildTierOf,
   reportSourceBuilt,
   reportSourceRefused,
} from "./run_telemetry";

/** The tiers a key check runs through, where its table lives. */
const COLOCATED = new ColocatedTier();
// Only its key checks. A build goes through the service's adapter, which
// routes a chained build through the run's collaborators; this one would not.
const STORAGE: Pick<StorageBuildStrategy, "checkKey" | "keyCheckSql"> =
   new StorageBuildStrategy();

/**
 * The key checks an auto-run runs over a table once it is written: a
 * rollup's key and its query sources' keys, a pair table's leaf key, and a
 * stored source's own key. Each records what it found; none fails the build.
 */
async function checkBuiltKeys(
   run: InstructedRun,
   persistSource: PersistSource,
   graphConnectionName: string,
   connection: MalloyConnection,
   { instruction, orchestrated, sourceEntityId }: GatedSource,
   entry: ManifestEntry,
): Promise<void> {
   const { compiled } = run;
   // A rollup's key checks (known limitation 29). Auto-run only; never
   // fails the build.
   if (!orchestrated && run.uniqueness) {
      await checkRollupKeys(
         run.uniqueness,
         compiled,
         persistSource,
         entry,
         connection,
         graphConnectionName,
         sourceEntityId,
         writtenTableIdentity(instruction, graphConnectionName),
      );
   }
   // A pair table (an inlined join_many): its leaf's key unique and never
   // NULL, checked over the leaf source's rows on the source's own
   // connection, which the served copy needs before it serves a count
   // through the path (known limitation 40). Never fails the build.
   if (!orchestrated && run.uniqueness) {
      await checkPairLeafKeys(
         run.uniqueness,
         pairLeafKeyChecksOf(
            compiled.tables[persistSource.sourceID]?.materialize,
         ),
         connection,
         persistSource.name,
         sourceEntityId,
         writtenTableIdentity(instruction, graphConnectionName),
      );
   }
   // A source stored in a destination, read there beside a joined
   // source's rollup through a join keyed on its primary key:
   // check that key over the rows its table holds.
   const stored =
      !orchestrated && run.uniqueness && entry.storageDestinationName
         ? storedKeyCheckOf(run, persistSource.sourceID, entry)
         : undefined;
   if (stored && run.uniqueness) {
      await checkAndRecordKey(
         run.uniqueness,
         sourceEntityId,
         "stored",
         () =>
            STORAGE.checkKey(
               {
                  destinationName: stored.destinationName,
                  destinationConnection: stored.destinationConnection,
                  environmentPath: run.environment.getEnvironmentPath(),
               },
               stored,
               stored.table,
            ),
         {
            notRun:
               "A stored source's primary key check did not run; its queries through a storage #(preaggregate) rollup read as a join_many are answered live",
            notUnique:
               "A stored source holds its primary key more than once or NULL; its queries through a storage #(preaggregate) rollup read as a join_many are answered live",
         },
         { source: persistSource.name, keyColumn: stored.keyColumn },
         // Recorded under its own sourceID.
         persistSource.sourceID,
      );
   }
}

/** What an instructed source's gates hand its build. */
interface GatedSource {
   instruction: BuildInstruction;
   orchestrated: boolean;
   /** The source's CONTENT address, which keys the manifest. */
   sourceEntityId: string;
   /** The physical table it writes ({@link physicalTargetKey}). */
   target: string;
}

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

/** Mark a table built at its author's `name=` as this package's; best-effort. */
async function markBuiltOwnedTable(
   run: InstructedRun,
   persistSource: PersistSource,
   ownedTarget: OwnedTableTarget,
   connection: MalloyConnection,
): Promise<void> {
   await atTableSite(ownedTarget, tableSiteContext(run, connection), (site) =>
      markOwnedTable(site, ownedTarget.physicalTableName, packageOwnerOf(run)),
   ).catch((err) =>
      logger.warn("Could not reach a built table to mark it", {
         sourceName: persistSource.name,
         error: errMessage(err),
      }),
   );
}

/**
 * Record a gated source's failure: unless it is refused for its connection, as
 * the source's failure, and, for a build that ran or an ownership refusal, as
 * its table's, so no source sharing the table writes it, and each takes the
 * same reason. Any other check before the build (uniqueness, ...) is cheap to
 * run again for the next source.
 */
function recordGatedSourceFailure(
   run: InstructedRun,
   persistSource: PersistSource,
   { instruction, sourceEntityId, target }: GatedSource,
   buildErr: unknown,
   buildStartedAt: number | undefined,
): void {
   const refused = { run, persistSource, instruction, sourceEntityId };
   if (refusedForItsConnection(refused, buildErr)) return;
   if (
      buildStartedAt !== undefined ||
      buildErr instanceof TableOwnershipError
   ) {
      run.failedTargets?.set(target, { sourceEntityId, err: buildErr });
   }
   recordInstructedBuildFailure(
      run,
      persistSource,
      instruction,
      sourceEntityId,
      buildErr,
      {
         kind: buildKindOf(run.compiled, persistSource.sourceID),
         tier: buildTierOf(instruction),
         // The method the build would have taken: a delta for a source whose
         // incremental refresh this run can act on.
         method:
            sourceIncrementalRefresh(run.incremental, persistSource, {
               physicalTableName: instruction.physicalTableName,
               storageDestinationName: instruction.destination,
               sourceEntityId,
            }) !== undefined
               ? "delta"
               : "full",
         ...(buildStartedAt !== undefined
            ? { durationMs: Math.round(performance.now() - buildStartedAt) }
            : {}),
      },
   );
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
          * One source's build. The service passes this, and the failed-run
          * reclaim, from its run collaborators, so a spec can hand it either.
          */
         buildOneSource: (
            options: SourceBuildOptions,
         ) => Promise<ManifestEntry>;
         reclaimStorageTablesFromFailedRun: (
            builtThisRun: ManifestEntry[],
            environment: BuildEnvironment,
            owner: { environmentId: string; packageName: string },
         ) => Promise<void>;
         /**
          * An auto-run refused a table at the author's `name=` because it is
          * not provably this package's (table_ownership.ts): the package's
          * older entries for that table are retired and the package rebound,
          * so it answers the source live instead of from rows another
          * package may have written. Absent: nothing is retired.
          */
         tableNotOwned: (
            environmentName: string,
            packageName: string,
            target: OwnedTableTarget,
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
   async run(options: InstructedBuildOptions): Promise<{
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
       * Refused sources whose table an eligible sibling built, by sourceID:
       * the table stands and serves its eligible sources, and only these
       * answer live.
       */
      refusedBesideBuilt: Record<string, RefusedSource>;
      /** The run metadata value for the uniqueness checks, when any ran. */
      uniquenessChecks?: ReturnType<UniquenessLedger["toMetadata"]>;
      /** See {@link InstructedRun.readFrom}. */
      readFrom?: ReadonlyMap<string, readonly string[]>;
   }> {
      const {
         compiled,
         environment,
         instructions,
         seedEntries,
         signal,
         strict = false,
         owner,
         buildMetadata,
         incremental,
         priorRefusals = [],
         clearedTiers,
         hostInstructed = false,
         warehouseReach = "recompute",
         uniqueness,
         packageRuns,
      } = options;
      const { builds, tables, connectionDigests, connections } = compiled;
      const index = indexInstructions(instructions);
      const run: InstructedRun = {
         compiled,
         environment,
         owner,
         buildMetadata,
         incremental,
         clearedTiers,
         ...(options.freshness ? { freshness: options.freshness } : {}),
         index,
         // Once per run: it depends only on the plan.
         planFacts: {
            ...chainedPlanFacts(
               tables,
               connectionDigests,
               compiled.tableFolds,
               // A host can imply a destination the annotation does not declare,
               // so the instruction decides a source's current tier.
               (sourceID, sourceEntityId) =>
                  index.bySourceID.get(sourceID) ??
                  index.bySourceEntityId.get(sourceEntityId),
            ),
            hostInstructed,
            warehouseReach,
            materializePlans: materializePlansOf(tables),
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
         failedTargets: new Map(),
         ...(uniqueness ? { uniqueness } : {}),
         ...(packageRuns ? { packageRuns } : {}),
         signal,
      };
      assertNoTableCollisions(
         builds,
         tables,
         connectionDigests,
         index,
         compiled.tableFolds,
      );

      let settled: ReturnType<typeof settleRefusals> = {
         refusalsInFailures: 0,
         refusedBesideBuilt: {},
      };
      try {
         await this.buildEveryTable(run, builds, tables, connections);

         settled = settleRefusals(run);

         // A run that produced nothing must not report success with errors
         // attached. Thrown inside the try so it takes the same cleanup path as
         // any other total failure.
         if (run.builtSources.length === 0 && run.failedReasons.length > 0) {
            throw failedRunError(
               run,
               new BuildRunFailedError(
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
               ),
            );
         }
         // Every instructed source was refused by its own gate: the refusal is
         // the answer. Not when a source its own gate admits was held back only
         // because a colocated sibling on its table is refused: the address
         // fails in `failures`, as an address with a refused source always
         // has, and the run is not failed for it.
         if (
            run.builtSources.length === 0 &&
            run.refusals.length > 0 &&
            !run.heldBackBySibling
         ) {
            throw failedRunError(run, allSourcesRefusedError(run.refusals));
         }
      } catch (err) {
         // A failed run's manifest records no entry, so its written tables are
         // unreachable to manifest-driven GC: reclaim them before rethrowing.
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

      // A rollup this run carried keeps its key check's record while its
      // table stands as that check found it (known limitation 29).
      if (uniqueness) carryRollupKeys(run, uniqueness);

      return {
         entries: run.entries,
         failures: run.failures,
         sourcesRefused: run.refusals.length,
         ...settled,
         ...(uniqueness?.toMetadata()
            ? { uniquenessChecks: uniqueness.toMetadata() }
            : {}),
         ...(run.readFrom ? { readFrom: run.readFrom } : {}),
      };
   }

   /**
    * Retire this package's entries for a table at its author's `name=` that
    * it cannot prove its own (`tableNotOwned`); best-effort.
    */
   private async retireNotOwned(
      run: InstructedRun,
      persistSource: PersistSource,
      ownedTarget: OwnedTableTarget,
   ): Promise<void> {
      const pkg = runPackageOf(run);
      if (!pkg) return;
      await this.seams
         .tableNotOwned(pkg.environmentName, pkg.packageName, ownedTarget)
         .catch((retireErr) => {
            if (retireErr instanceof LockOrderError) throw retireErr;
            logger.warn(
               "Could not retire a package's entries for a table it does not own",
               {
                  sourceName: persistSource.name,
                  error: errMessage(retireErr),
               },
            );
         });
   }

   /**
    * Refuse an auto-run's build at the author's `name=` unless the name is
    * free or this package's (assertTableOwnable). This package's records are
    * read only for a table with no mark. A `name=` view an earlier release
    * of this package left at the name is dropped here, under the name's lock
    * (leftover_name_views.ts), so the build puts its table in its place.
    */
   private async assertOwnable(
      run: InstructedRun,
      // The source whose `name=` this is, which every message names: the
      // refusal is shared with each source on the table (failedTargets).
      sourceName: string,
      target: OwnedTableTarget,
      connection: MalloyConnection,
   ): Promise<string[] | undefined> {
      const packageRuns = run.packageRuns;
      const pkg = runPackageOf(run);
      const owner = packageOwnerOf(run);
      let reached = false;
      const replacesView = await atTableSite(
         target,
         tableSiteContext(run, connection),
         async (site) => {
            reached = true;
            const { ownView } = await assertTableOwnable(
               site,
               target,
               owner,
               sourceName,
               async () =>
                  !!packageRuns &&
                  !!pkg &&
                  packageRecordsName(await packageRuns(), target),
            );
            if (!ownView) return undefined;
            // This package's own view from an earlier release: replaced by
            // the table only while the warehouse proves it still reads the
            // table its mark names (a view repointed in place keeps its
            // comment, and is someone's).
            const view = await provenOwnView(
               site,
               target.physicalTableName,
               owner,
            );
            if (!view) {
               throw new TableOwnershipError(
                  TABLE_OWNED_ELSEWHERE,
                  `'${sourceName}' is not built (${TABLE_OWNED_ELSEWHERE}): ` +
                     `its name= '${target.physicalTableName}' holds a name= view ` +
                     `this package made in an earlier release, which no longer ` +
                     `provably reads the table its mark names, so it is left as ` +
                     `it is. It is answered live. Drop the view if nothing needs it.`,
               );
            }
            // DuckDB and Postgres drop it in the build's own transaction.
            const statements = replaceOwnViewStatements(view, site);
            if (statements) return statements;
            if (
               !(await dropOwnNameView(
                  site,
                  target.physicalTableName,
                  owner,
                  view.target,
               ))
            ) {
               throw new Error(
                  `'${sourceName}' is not built: its name= ` +
                     `'${target.physicalTableName}' changed while it was ` +
                     `checked. The next build checks it again.`,
               );
            }
            logger.info(
               "Dropped this package's leftover name= view to build its table there",
               {
                  sourceName,
                  physicalTableName: target.physicalTableName,
               },
            );
            return undefined;
         },
      );
      if (!reached) {
         await assertTableOwnable(
            undefined,
            target,
            owner,
            sourceName,
            async () => false,
         );
      }
      return replacesView;
   }

   /**
    * Build every planned table in the plan's order, then each rollup that may
    * read its base's stored table, so a base this run builds is there to
    * read. Nothing a run builds reads a rollup's table.
    */
   private async buildEveryTable(
      run: InstructedRun,
      builds: CompiledBuildPlan["builds"],
      tables: CompiledBuildPlan["tables"],
      connections: CompiledBuildPlan["connections"],
   ): Promise<void> {
      const deferred: {
         persistSource: PersistSource;
         connectionName: string;
         connection: MalloyConnection;
      }[] = [];
      for (const build of builds) {
         const connection = connections.get(build.connectionName);
         if (!connection) {
            throw new BadRequestError(
               `Connection '${build.connectionName}' not found`,
            );
         }
         for (const persistSource of sourcesInBuildOrder(build, tables)) {
            // A rollup that may read its base's stored table builds once
            // every other table has, so a base this run builds is there
            // to read. Nothing a run builds reads a rollup's table.
            const table = tables[persistSource.sourceID];
            if (table?.kind === "rollup" && table.overStoredBase) {
               deferred.push({
                  persistSource,
                  connectionName: build.connectionName,
                  connection,
               });
               continue;
            }
            await this.buildInRun(
               run,
               persistSource,
               build.connectionName,
               connection,
            );
         }
      }
      for (const later of deferred) {
         await this.buildInRun(
            run,
            later.persistSource,
            later.connectionName,
            later.connection,
         );
      }
   }

   /** One source's turn in the run's loop, under its span. */
   private async buildInRun(
      run: InstructedRun,
      persistSource: PersistSource,
      connectionName: string,
      connection: MalloyConnection,
   ): Promise<void> {
      if (run.signal?.aborted) throw new BuildCancelledError();
      await withSpan(
         "publisher.materialization.build_source",
         // Nested under the run's span, which names the package.
         { "malloy.source": persistSource.name },
         () =>
            this.buildInstructedSource(
               run,
               persistSource,
               connectionName,
               connection,
            ),
      );
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
      const gated = this.gateInstructedSource(
         run,
         persistSource,
         graphConnectionName,
      );
      if (!gated) return;
      const { instruction, sourceEntityId, target } = gated;
      const { compiled } = run;
      const built = await this.buildGatedSource(
         run,
         (await overStoredBaseSource(run, persistSource, gated)) ??
            persistSource,
         graphConnectionName,
         connection,
         gated,
      );
      if (!built) return;
      const entry = withRollupUpstreamReuse(
         run,
         persistSource,
         sourceEntityId,
         built,
      );
      run.builtSources.push(persistSource.name);
      reportSourceBuilt(
         {
            kind: buildKindOf(compiled, persistSource.sourceID),
            tier: entry.storageDestinationName ? "storage" : "colocated",
            sourceEntityId,
            sourceName: persistSource.name,
            physicalTableName: instruction.physicalTableName,
         },
         entry,
      );
      run.entries[sourceEntityId] = entry;
      run.builtAddresses.add(sourceEntityId);
      await checkBuiltKeys(
         run,
         persistSource,
         graphConnectionName,
         connection,
         gated,
         entry,
      );
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
    * The gates before one instructed source's build: its instruction, its
    * eligibility, a colocated sibling's refusal, the storage re-assert, and
    * another route to a table this run built or failed. What the build needs,
    * or undefined when the source is not built (any refusal recorded).
    */
   private gateInstructedSource(
      run: InstructedRun,
      persistSource: PersistSource,
      graphConnectionName: string,
   ): GatedSource | undefined {
      const { compiled } = run;
      const resolved = resolveInstruction(
         run.index,
         persistSource,
         compiled.connectionDigests,
         compiled.tableFolds,
      );
      if (!resolved) return undefined;
      const { instruction, orchestrated } = resolved;

      if (this.refuseIneligibleInstructedSource(run, persistSource, resolved)) {
         return undefined;
      }

      // The manifest is keyed by the CONTENT sourceEntityId (what Malloy
      // recomputes to resolve upstream references), not the instruction's
      // identity. A sourceID match has not computed it yet.
      const sourceEntityId =
         resolved.sourceEntityId ??
         computeSourceEntityId(
            persistSource,
            compiled.connectionDigests,
            compiled.tables[persistSource.sourceID]?.target,
            compiled.tableFolds,
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
            return undefined;
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
            { connectionTypes: run.compiled.connectionTypes },
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
         return undefined;
      }
      // Another route to a table whose build failed this run: the same
      // content fails the same way, so the source takes that failure (its
      // reason and code) without running the build again.
      const failedHere = run.failedTargets?.get(target);
      if (failedHere && failedHere.sourceEntityId === sourceEntityId) {
         recordInstructedBuildFailure(
            run,
            persistSource,
            instruction,
            sourceEntityId,
            failedHere.err,
            {
               kind: buildKindOf(compiled, persistSource.sourceID),
               tier: buildTierOf(instruction),
               method: "full",
            },
         );
         return undefined;
      }
      return { instruction, orchestrated, sourceEntityId, target };
   }

   /**
    * One gated source's build, guarded: its `name=` ownership, its inlined
    * join_one checks, the build itself and its ownership mark. The entry it
    * built, or undefined when it failed (its failure recorded).
    */
   private async buildGatedSource(
      run: InstructedRun,
      persistSource: PersistSource,
      graphConnectionName: string,
      connection: MalloyConnection,
      { instruction, orchestrated, sourceEntityId, target }: GatedSource,
   ): Promise<ManifestEntry | undefined> {
      const { compiled } = run;
      // An auto-run's table at the author's own `name=`, which another
      // package on the same schema may hold (table_ownership.ts). Every other
      // auto-run name is the package's own by construction.
      const ownedTarget = authorNamedTarget(
         run,
         persistSource,
         instruction,
         orchestrated,
      );
      let entry: ManifestEntry | undefined;
      // Set once the build itself starts: a source that fails before it
      // (ownership, uniqueness) ran no build to time.
      let buildStartedAt: number | undefined;
      const checkBuildAndMark = async (): Promise<ManifestEntry> => {
         // A `name=` another package holds fails the source before anything
         // is written, the uniqueness check included. This package's own
         // leftover view at the name is dropped by the build's own
         // transaction before its table takes the name (DuckDB, Postgres).
         const replacesView = ownedTarget
            ? await this.assertOwnable(
                 run,
                 authorNamedInstructions(run).get(instruction) ??
                    persistSource.name,
                 ownedTarget,
                 connection,
              )
            : undefined;

         // An inlined join_one must match at most one row (P8): checked on
         // the source's own connection before anything is written.
         // Auto-run skips one whose joined table provably did not change
         // since it last passed (inline_uniqueness.ts).
         await assertInlinedJoinsUnique(
            compiled.tables[persistSource.sourceID]?.materialize,
            connection,
            orchestrated ? undefined : run.uniqueness,
            orchestrated || !run.uniqueness
               ? undefined
               : (check) =>
                    storedJoinOf(
                       run,
                       persistSource,
                       graphConnectionName,
                       check,
                    ),
         );

         // From here this source's table may change: no pass recorded
         // against it holds any longer, whether or not the build succeeds.
         run.uniqueness?.markWritten(
            sourceEntityId,
            writtenTableIdentity(instruction, graphConnectionName),
         );

         buildStartedAt = performance.now();
         let built = await this.seams.buildOneSource({
            persistSource,
            instruction,
            connection,
            manifest: run.manifest,
            environment: run.environment,
            builtEntries: run.entries,
            buildMetadata: run.buildMetadata,
            incremental: run.incremental,
            // The CONTENT address, distinct from the instruction's
            // caller-assigned identity: the ledger is keyed by it so a
            // boundary can never be read against different SQL.
            contentSourceEntityId: sourceEntityId,
            planFacts: run.planFacts,
            ...(replacesView ? { replacesView } : {}),
         });
         // A manifest travels without its build plan, so this is the only
         // signal that a table belongs to a rollup in no model file; served
         // as an ordinary binding it would be silently dropped (see
         // preaggregation_serve_bindings).
         if (compiled.tables[persistSource.sourceID]?.rollup) {
            built = { ...built, origin: "preaggregate" as const };
         }
         if (ownedTarget) {
            await markBuiltOwnedTable(
               run,
               persistSource,
               ownedTarget,
               connection,
            );
         }
         return built;
      };
      try {
         // An instructed destination that cannot be honored REFUSES, never
         // falls through to a colocated build: that would write the caller's
         // name into the source's own warehouse and answer success.
         assertInstructedDestinationWritable(
            persistSource,
            instruction,
            compiled,
         );
         // At an author's `name=`, held from the ownership check until the
         // mark is written, so another package building the same name at
         // once waits, then sees the mark.
         entry = ownedTarget
            ? await withOwnedNameLock(
                 ownedTarget.physicalTableName,
                 checkBuildAndMark,
              )
            : await checkBuildAndMark();
      } catch (buildErr) {
         // Retired once the name's lock is let go: the retire takes the
         // table, package and version locks (run/README.md).
         if (ownedTarget && buildErr instanceof TableOwnershipError) {
            await this.retireNotOwned(run, persistSource, ownedTarget);
         }
         recordGatedSourceFailure(
            run,
            persistSource,
            { instruction, orchestrated, sourceEntityId, target },
            buildErr,
            buildStartedAt,
         );
         return undefined;
      }
      return entry;
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
         // Its own placement, never a sibling's (assertPlacementNamed).
         assertPlacementNamed(persistSource);
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

/**
 * Where an auto-run writes a source's table at an author's own `name=`, or
 * undefined: a host-instructed build (the host chose the name), a name
 * Publisher chose (its readable name, hashed over the package and the
 * version, so no other package writes it), or a run that cannot say whose it
 * is. Decided by the instruction, not the source: every source sharing the
 * address builds the one instruction. An auto-run gives each `name=` its own
 * address (foldDistinctTableNames); an instruction a run is handed may still
 * name one table for sources that differ, so a sibling with no `name=` of its
 * own that writes an author's name is checked as the author's source is.
 */
function authorNamedTarget(
   run: InstructedRun,
   persistSource: PersistSource,
   instruction: BuildInstruction,
   orchestrated: boolean,
): OwnedTableTarget | undefined {
   if (orchestrated || run.planFacts.hostInstructed) return undefined;
   if (!runPackageOf(run)) return undefined;
   if (!authorNamedInstructions(run).has(instruction)) return undefined;
   return {
      physicalTableName: instruction.physicalTableName,
      connectionName: persistSource.connectionName,
      ...(instruction.destination
         ? { storageDestinationName: instruction.destination }
         : {}),
   };
}

/** Each run's {@link authorNamedInstructions}, computed once. */
const authorNamedByRun = new WeakMap<
   InstructedRun,
   Map<BuildInstruction, string>
>();

/**
 * The run's instructions written at an author's own `name=`, each with the
 * name of the first source that declares it: those a source with that
 * `name=` resolves to, at that name.
 */
function authorNamedInstructions(
   run: InstructedRun,
): Map<BuildInstruction, string> {
   let named = authorNamedByRun.get(run);
   if (!named) {
      named = new Map();
      for (const { source } of Object.values(run.compiled.tables)) {
         if (!hasAuthorTableName(run.compiled, source)) continue;
         const instruction = resolveInstruction(
            run.index,
            source,
            run.compiled.connectionDigests,
            run.compiled.tableFolds,
         )?.instruction;
         if (
            instruction?.physicalTableName === selfAssignTableName(source) &&
            !named.has(instruction)
         ) {
            named.set(instruction, source.name);
         }
      }
      authorNamedByRun.set(run, named);
   }
   return named;
}

/** The environment and package a run builds, when it says. */
function runPackageOf(
   run: InstructedRun,
): { environmentName: string; packageName: string } | undefined {
   const environmentName =
      run.buildMetadata?.context.environment ??
      run.environment.getEnvironmentName();
   const packageName =
      run.buildMetadata?.context.package ?? run.owner?.packageName;
   return environmentName && packageName
      ? { environmentName, packageName }
      : undefined;
}

/** The package a run's tables are marked as built by. */
function packageOwnerOf(run: InstructedRun): string {
   return packageOwner(
      run.buildMetadata?.context.environment ??
         run.environment.getEnvironmentName() ??
         "",
      run.buildMetadata?.context.package ?? run.owner?.packageName ?? "",
   );
}

/**
 * How a run reaches a table's site: the source's connection (the one this
 * build holds, when given), or the destination the run builds into.
 */
export function tableSiteContext(
   run: Pick<InstructedRun, "compiled" | "environment" | "buildMetadata">,
   connection?: MalloyConnection,
): TableSiteContext {
   const context = run.buildMetadata?.context;
   return {
      // The run's own statements carry its metadata, as its builds do.
      ...(context
         ? {
              runOptions: (name: string) =>
                 statementRunOptions(run.environment, name, context),
           }
         : {}),
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
 * knows which addresses it built (instructed_refused_sibling.spec.ts). A
 * base and its `extend` share one content address, and the gate decides per
 * source. A
 * refusal on an address an eligible sibling built fails only the refused
 * source: the table stands, its eligible sources serve it, and the refused
 * one answers live (routing skips a gated entry point). It stays out of
 * `failures` and `entries`, which the control plane reads by address, and is
 * reported per source instead. A refusal on an address nothing built is
 * recorded once per address. Neither depends on run order.
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
      // Once per address; a build failure already there stands.
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
 * A run that built nothing, as the error it throws, carrying the manifest its
 * FAILED record keeps: every source's own failure, a refusal included
 * ({@link settleRefusals}). No entries: a seeded entry names a table an
 * earlier run built, and a failure's deprecated mirror names the previous
 * generation's, so neither belongs to this run, and deleting its record must
 * never drop them.
 */
function failedRunError<E extends Error>(run: InstructedRun, err: E): E {
   return withFailedRunManifest(err, { failures: { ...run.failures } });
}

/**
 * Record a `storage=` source whose connection the build cannot read as
 * refused, as the plan's gate would have (storage_build_strategy.ts), so the
 * run goes on. Returns whether `err` was that refusal.
 */
function refusedForItsConnection(
   source: {
      run: InstructedRun;
      persistSource: PersistSource;
      instruction: BuildInstruction;
      sourceEntityId: string;
   },
   err: unknown,
): boolean {
   if (
      !(err instanceof MaterializationEligibilityError) ||
      err.reason !== "storage_source_unsupported"
   ) {
      return false;
   }
   const { run, persistSource, instruction, sourceEntityId } = source;
   recordPendingRefusal(
      run,
      persistSource,
      instruction,
      sourceEntityId,
      err,
      true,
   );
   return true;
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
   reportSourceRefused(
      {
         kind: buildKindOf(run.compiled, persistSource.sourceID),
         tier: buildTierOf(instruction),
         sourceEntityId: instruction.sourceEntityId,
         sourceName: persistSource.name,
      },
      err.reason,
   );
   run.failedBySourceID.set(persistSource.sourceID, {
      name: persistSource.name,
      reason,
      retryable: false,
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
         run.compiled.tables[persistSource.sourceID]?.rollup
            ? "preaggregate"
            : instruction.destination
              ? "storage"
              : "colocated",
         run.compiled.tables[persistSource.sourceID]?.modelPath,
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
      for (const [id, { source, target }] of Object.entries(compiled.tables)) {
         let sourceAddress: string;
         try {
            sourceAddress = computeSourceEntityId(
               source,
               compiled.connectionDigests,
               target,
               compiled.tableFolds,
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
         const source = compiled.tables[id]?.source;
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

/** The table an instructed build writes, as {@link tableIdentityKey} names it. */
function writtenTableIdentity(
   instruction: BuildInstruction,
   connectionName: string,
): string {
   return instruction.destination
      ? tableIdentityKey(
           "destination",
           instruction.destination,
           instruction.physicalTableName,
        )
      : tableIdentityKey(
           "connection",
           connectionName,
           instruction.physicalTableName,
        );
}

/**
 * The joined source of `check` as the parent's build reads it, when that is a
 * persisted table, or undefined (the check then runs as compiled, every
 * build). Every condition is evidence, not assumption:
 *
 * - the joined source is itself a persist source of this plan, and not an
 *   incremental one (whose table changes in place under one address);
 * - the run holds a warehouse table for its address on the connection the
 *   check runs on (a `storage=` table is never read by a warehouse build);
 * - the parent's build SQL changes when that table is taken out of the
 *   manifest, so the parent's build reads it;
 * - the check, compiled over the same manifest, reads it too (its SQL differs
 *   from the check as planned, which reads the source live).
 *
 * Anything that throws is doubt, and resolves to undefined.
 */
async function storedJoinOf(
   run: InstructedRun,
   parent: PersistSource,
   connectionName: string,
   check: InlineUniquenessCheck,
): Promise<StoredJoin | undefined> {
   try {
      const { compiled } = run;
      const compile = compiled.keyCheckCompilers?.get(
         keyCheckCompilerKey(parent.sourceID, check.queryName),
      );
      const joined = compiled.tables[check.joinedSourceID];
      if (!compile || !joined || !check.sql) return undefined;
      if (run.incremental?.declarations[check.joinedSourceID]) return undefined;
      const address = computeSourceEntityId(
         joined.source,
         compiled.connectionDigests,
         joined.target,
         compiled.tableFolds,
      );
      const entry = run.entries[address];
      if (
         !entry?.physicalTableName ||
         entry.storageDestinationName ||
         entry.connectionName !== connectionName
      ) {
         return undefined;
      }
      const reduced = manifestExcludingStorage(
         run.manifest,
         run.entries,
         run.planFacts,
      );
      if (!(address in reduced.entries)) return undefined;
      const withTable = { ...reduced, strict: false };
      const withoutTable = {
         ...withTable,
         entries: Object.fromEntries(
            Object.entries(reduced.entries).filter(([id]) => id !== address),
         ),
      };
      const target = compiled.tables[parent.sourceID]?.target;
      if (
         buildSQLOf(parent, withTable, target) ===
         buildSQLOf(parent, withoutTable, target)
      ) {
         return undefined;
      }
      const sql = await compile(withTable, compiled.connectionDigests);
      if (sql === check.sql) return undefined;
      return {
         sql,
         address,
         table: tableIdentityKey(
            "connection",
            connectionName,
            entry.physicalTableName,
         ),
      };
   } catch (err) {
      logger.debug(
         "Inline uniqueness check: the joined table could not be proven; checking as compiled",
         { sourceName: parent.name, join: check.join, error: errMessage(err) },
      );
      return undefined;
   }
}

/**
 * The key checks of a rollup just built, recorded against the table written:
 * its own (a join_one keyed on its grain, known limitation 29) and its base's
 * primary key, both over the base's rows on the source's own connection, the
 * rows a build from the author's definition groups; and the primary key of
 * each query source that reads a colocated rollup through a join_many on it.
 *
 * A build that stacked on a stored upstream (`upstreamReuse: "reused"`)
 * grouped that table's rows instead, which a check over the base's own rows
 * may not describe (a rebuild of the rollup alone, over a parent carried from
 * an earlier run, whose stored rows predate a duplicate). The checks over the
 * base are then not run: no verdict, so the join branch reads the key as unknown
 * and declines (`COUNT_SEMANTICS`) rather than trust another set of rows.
 */
async function checkRollupKeys(
   ledger: UniquenessLedger,
   compiled: CompiledBuildPlan,
   persistSource: PersistSource,
   entry: ManifestEntry,
   connection: MalloyConnection,
   connectionName: string,
   address: string,
   table: string,
): Promise<void> {
   const { sourceID } = persistSource;
   const groupsBaseRows = entry.upstreamReuse !== "reused";
   const baseSourceID = compiled.tables[sourceID]?.rollup?.baseSourceID;
   const own = rollupKeyCheckOf(compiled, sourceID);
   if (own && !groupsBaseRows) {
      logger.info(
         "A #(preaggregate) rollup was built over a stored upstream, so its key check over the base's rows is not run; queries through its join_one that need the key unique are answered live",
         { rollup: persistSource.name, keyColumn: own.keyColumn },
      );
   }
   if (own && groupsBaseRows) {
      await checkAndRecordKey(
         ledger,
         address,
         "rollup",
         () => COLOCATED.checkKey({ connection }, own, table),
         {
            notRun:
               "A #(preaggregate) rollup's key check did not run; queries through its join_one that need the key unique are answered live",
            notUnique:
               "A #(preaggregate) rollup's base holds its join_one key more than once; queries that read that join_one beside other aggregates are answered live",
         },
         { rollup: persistSource.name, keyColumn: own.keyColumn },
      );
   }
   await checkParentKeys(
      ledger,
      rollupParentKeyChecksOf(compiled, sourceID).filter(
         (check) => groupsBaseRows || check.sourceID !== baseSourceID,
      ),
      connection,
      connectionName,
      persistSource.name,
      address,
      table,
   );
}

/** The key check a run builds for the rollup `sourceID`, if any (known limitation 29). */
function rollupKeyCheckOf(
   compiled: CompiledBuildPlan,
   sourceID: string,
): RollupKeyCheck | undefined {
   return (
      compiled.tables[sourceID]?.rollup as
         | { keyCheck?: RollupKeyCheck }
         | undefined
   )?.keyCheck;
}

/** The query source key checks a run builds for the rollup `sourceID`. */
function rollupParentKeyChecksOf(
   compiled: CompiledBuildPlan,
   sourceID: string,
): ParentKeyCheck[] {
   return (
      (
         compiled.tables[sourceID]?.rollup as
            | { parentKeyChecks?: ParentKeyCheck[] }
            | undefined
      )?.parentKeyChecks ?? []
   );
}

/**
 * Run each query source's primary key check of a built rollup, on the
 * rollup's connection, and record what it found beside the rollup. A check
 * on another connection, or one that does not run, records nothing: the
 * join-to-rollup branch then reads that source's key as unknown and declines.
 */
async function checkParentKeys(
   ledger: UniquenessLedger,
   checks: readonly ParentKeyCheck[],
   connection: MalloyConnection,
   connectionName: string,
   rollupName: string,
   address: string,
   table: string,
): Promise<void> {
   for (const check of checks) {
      if (check.connectionName !== connectionName) continue;
      await checkAndRecordKey(
         ledger,
         address,
         "parent",
         () => COLOCATED.checkKey({ connection }, check, table),
         {
            notRun:
               "A query source's primary key check did not run; its queries through a #(preaggregate) rollup read as a join_many are answered live",
            notUnique:
               "A query source holds its primary key more than once or NULL; its queries through a #(preaggregate) rollup read as a join_many are answered live",
         },
         {
            rollup: rollupName,
            source: check.sourceID,
            keyColumn: check.keyColumn,
         },
         check.sourceID,
      );
   }
}

/**
 * Run each leaf key check of a built pair table on the source's connection
 * and record what it found beside the table, under the leaf source's
 * sourceID. A check that does not run records nothing: the served copy then
 * reads the leaf's key as unknown and keeps declining a count through the
 * path.
 */
async function checkPairLeafKeys(
   ledger: UniquenessLedger,
   checks: readonly { sourceID: string; keyColumn: string; sql: string }[],
   connection: MalloyConnection,
   sourceName: string,
   address: string,
   table: string,
): Promise<void> {
   for (const check of checks) {
      await checkAndRecordKey(
         ledger,
         address,
         "pair_leaf",
         () => COLOCATED.checkKey({ connection }, check, table),
         {
            notRun:
               "A pair table's leaf key check did not run; counts through its inlined join_many are answered live",
            notUnique:
               "An inlined join_many's joined source holds its primary key more than once or NULL; counts through it are answered live",
         },
         { source: sourceName, keyColumn: check.keyColumn },
         check.sourceID,
      );
   }
}

/**
 * The check of a stored table's declared primary key, when the plan asks for
 * one (the table's `storedKeyCheck`) and the table holds that column: one row when a
 * value repeats or is NULL. Over every row the table holds: a term the
 * served copy applies at read only narrows them, so a key unique here is
 * unique in whatever a caller reads.
 */
function storedKeyCheckOf(
   run: InstructedRun,
   sourceID: string,
   entry: ManifestEntry,
):
   | {
        keyColumn: string;
        sql: string;
        table: string;
        destinationName: string;
        destinationConnection: ReturnType<
           InstructedRun["environment"]["getStorageDestination"]
        >;
     }
   | undefined {
   const keyColumn = run.compiled.tables[sourceID]?.storedKeyCheck;
   const destinationName = entry.storageDestinationName;
   if (!keyColumn || !destinationName || !entry.physicalTableName) {
      return undefined;
   }
   if (!(entry.schema ?? []).some((c) => c.name === keyColumn)) {
      return undefined;
   }
   let destinationConnection;
   try {
      destinationConnection =
         run.environment.getStorageDestination(destinationName);
   } catch {
      return undefined;
   }
   return {
      keyColumn,
      sql: STORAGE.keyCheckSql(
         { destinationName, destinationConnection },
         entry.physicalTableName,
         keyColumn,
      ),
      table: tableIdentityKey(
         "destination",
         destinationName,
         entry.physicalTableName,
      ),
      destinationName,
      destinationConnection,
   };
}

/**
 * Run one key check, warn when it did not run or found the key repeated, and
 * record what it found under `address` (and `sourceID`, for a check of a
 * source other than the table's own). A check that does not run, or throws,
 * on either tier, reads as unknown: it warns, records nothing, and never
 * fails the build.
 */
export async function checkAndRecordKey(
   ledger: UniquenessLedger,
   address: string,
   kind: KeyCheckKind,
   runCheck: () => Promise<KeyCheckRecord | undefined>,
   messages: { notRun: string; notUnique: string },
   logContext: Record<string, unknown>,
   sourceID?: string,
): Promise<void> {
   let record: KeyCheckRecord | undefined;
   try {
      record = await runCheck();
   } catch (err) {
      recordKeyCheck(kind, "not_run");
      logger.warn(messages.notRun, { ...logContext, error: errMessage(err) });
      return;
   }
   if (!record) {
      recordKeyCheck(kind, "not_run");
      logger.warn(messages.notRun, logContext);
      return;
   }
   recordKeyCheck(kind, record.unique ? "unique" : "not_unique");
   if (!record.unique) logger.warn(messages.notUnique, logContext);
   ledger.recordRollupKey(
      address,
      sourceID !== undefined ? { ...record, sourceID } : record,
   );
}

/**
 * For each rollup with a key check whose entry this run carried rather than
 * built, keep the previous run's record when it still describes the table
 * ({@link UniquenessLedger.carryRollupKey}).
 */
function carryRollupKeys(run: InstructedRun, ledger: UniquenessLedger): void {
   const { compiled } = run;
   for (const [sourceID, table] of Object.entries(compiled.tables)) {
      if (!table.rollup) continue;
      const check = rollupKeyCheckOf(compiled, sourceID);
      const parentChecks = rollupParentKeyChecksOf(compiled, sourceID);
      const { source } = table;
      if (!check && parentChecks.length === 0) continue;
      const address = computeSourceEntityId(
         source,
         compiled.connectionDigests,
         table.target,
         compiled.tableFolds,
      );
      const entry = run.entries[address];
      if (!entry?.physicalTableName || run.builtAddresses.has(address)) {
         continue;
      }
      // A rollup in a destination carries its own key check and its base's
      // primary key check (`compileParentKeyChecks`), recorded against its
      // table there.
      const stored = entry.storageDestinationName;
      const tableKey = stored
         ? tableIdentityKey("destination", stored, entry.physicalTableName)
         : tableIdentityKey(
              "connection",
              source.connectionName,
              entry.physicalTableName,
           );
      if (check) {
         ledger.carryRollupKey(
            address,
            tableKey,
            checkDigest(tableKey, check.sql),
         );
      }
      for (const parent of parentChecks) {
         ledger.carryRollupKey(
            address,
            tableKey,
            checkDigest(tableKey, parent.sql),
            parent.sourceID,
         );
      }
   }
   // A pair table's leaf key checks, while its table stands.
   for (const { source, target, materialize } of Object.values(
      compiled.tables,
   )) {
      const checks = pairLeafKeyChecksOf(materialize);
      if (checks.length === 0) continue;
      const address = computeSourceEntityId(
         source,
         compiled.connectionDigests,
         target,
         compiled.tableFolds,
      );
      const entry = run.entries[address];
      if (!entry?.physicalTableName || run.builtAddresses.has(address)) {
         continue;
      }
      const table = entry.storageDestinationName
         ? tableIdentityKey(
              "destination",
              entry.storageDestinationName,
              entry.physicalTableName,
           )
         : tableIdentityKey(
              "connection",
              source.connectionName,
              entry.physicalTableName,
           );
      for (const check of checks) {
         ledger.carryRollupKey(
            address,
            table,
            checkDigest(table, check.sql),
            check.sourceID,
         );
      }
   }
   // A stored source's own key check, while its table stands.
   for (const [sourceID, table] of Object.entries(compiled.tables)) {
      if (table.storedKeyCheck === undefined) continue;
      const address = computeSourceEntityId(
         table.source,
         compiled.connectionDigests,
         table.target,
         compiled.tableFolds,
      );
      const entry = run.entries[address];
      if (!entry || run.builtAddresses.has(address)) continue;
      const check = storedKeyCheckOf(run, sourceID, entry);
      if (!check) continue;
      ledger.carryRollupKey(
         address,
         check.table,
         checkDigest(check.table, check.sql),
         sourceID,
      );
   }
}

/**
 * Whether a rollup with a declared window may read its base's table: the
 * base was built in this run, or its data anchor is known and inside the
 * rollup's window at the run's start. A table of unknown age is never read
 * as if it were new; the rollup builds from the warehouse instead. A rollup
 * that declares no window reads any current table of its base.
 */
function baseFreshEnough(
   run: InstructedRun,
   rollupAddress: string,
   baseAddress: string,
   baseEntry: ManifestEntry,
): boolean {
   const window = run.freshness?.windowSecondsAt(rollupAddress);
   if (window === undefined) return true;
   if (run.builtAddresses.has(baseAddress)) return true;
   if (baseEntry.dataAsOf === undefined) return false;
   return (
      freshnessOf(
         { dataAsOf: baseEntry.dataAsOf, freshnessWindowSeconds: window },
         run.freshness!.at,
      ) === "fresh"
   );
}

/**
 * The source a colocated rollup builds from when this run holds its base's
 * table: the rollup compiled over that table (rollup_compile.ts), so its
 * build reads the table rather than the warehouse, through the build
 * manifest as any chained build does. Only when every one of these holds,
 * otherwise undefined and the rollup builds from the warehouse as planned:
 *
 *  - the rollup builds colocated, and its base is a colocated `#@ persist`
 *    source or a colocated `#(materialize)` plain table (the plan offers the
 *    compile only then);
 *  - the run's entries hold the base's table at the address the plan gives
 *    it, built this run or carried under the reuse rules, colocated and at
 *    the tier the base declares now. The colocated gate refused nothing on
 *    that address, or it would not be built or carried, and a table's rows
 *    do not depend on any gate: each is applied when a query reads it;
 *  - with a declared window, the base's table is new enough
 *    ({@link baseFreshEnough}), whoever instructed the run;
 *  - the compiled source builds the planned table ({@link buildsPlannedTable}).
 *
 * Records the base as what the rollup read, for its freshness anchor.
 */
async function overStoredBaseSource(
   run: InstructedRun,
   persistSource: PersistSource,
   gated: Pick<GatedSource, "instruction" | "sourceEntityId">,
): Promise<PersistSource | undefined> {
   const { compiled } = run;
   const table = compiled.tables[persistSource.sourceID];
   if (table?.kind !== "rollup" || !table.overStoredBase) return undefined;
   if (gated.instruction.destination) return undefined;
   const baseID = table.rollup.baseSourceID;
   const base = baseID === undefined ? undefined : compiled.tables[baseID];
   if (!base || (base.kind !== "persist" && base.kind !== "materialize")) {
      return undefined;
   }
   let baseAddress: string;
   try {
      baseAddress = computeSourceEntityId(
         base.source,
         compiled.connectionDigests,
         base.target,
         compiled.tableFolds,
      );
   } catch {
      return undefined;
   }
   const baseEntry = run.entries[baseAddress];
   if (
      !baseEntry?.physicalTableName ||
      baseEntry.storageDestinationName ||
      !upstreamEntryIsCurrent(baseAddress, baseEntry, run.planFacts)
   ) {
      return undefined;
   }
   if (!baseFreshEnough(run, gated.sourceEntityId, baseAddress, baseEntry)) {
      return undefined;
   }
   let variant: OverStoredBase | undefined;
   try {
      variant = await table.overStoredBase();
   } catch {
      return undefined;
   }
   if (
      !variant ||
      (base.kind === "materialize") !== (variant.baseBuildSource !== undefined)
   ) {
      return undefined;
   }
   try {
      if (
         !buildsPlannedTable(run, persistSource, variant, {
            base: base.source,
            baseAddress,
            rollupAddress: gated.sourceEntityId,
         })
      ) {
         return undefined;
      }
   } catch {
      return undefined;
   }
   (run.readFrom ??= new Map()).set(gated.sourceEntityId, [baseAddress]);
   return variant.source;
}

/**
 * Whether `variant`, the rollup compiled over its base's stored table, builds
 * the table the plan gives the rollup `planned`:
 *
 *  - over a `#@ persist` base, it has the rollup's own planned address: its
 *    SQL, the base's inlined, is the plan's;
 *  - over a `#(materialize)` base, the base is re-rooted on its generated
 *    build source, which moves the rollup's SQL, so its own address cannot
 *    match. The build source must instead be at the base's planned address
 *    (the same build source, so the same table), the rollup's SQL rendered
 *    over the base's entry alone must read that table, and its columns must
 *    be the planned rollup's, by name, type and numeric subtype. The rows
 *    are the base's either way, as an extension's build over its parent's
 *    table (P10).
 *
 * The three `#(materialize)` checks are defensive: a compile that re-roots
 * the base as planned passes all three, so none is false in practice. Each
 * guards a drift between the second compile and the plan (another build
 * source, a rollup that no longer reads the base's table, other columns) and
 * fails closed, to the warehouse build. Exported for its unit spec.
 */
export function buildsPlannedTable(
   run: InstructedRun,
   planned: PersistSource,
   variant: OverStoredBase,
   at: { base: PersistSource; baseAddress: string; rollupAddress: string },
): boolean {
   const { compiled } = run;
   if (!variant.baseBuildSource) {
      return (
         computeSourceEntityId(
            variant.source,
            compiled.connectionDigests,
            undefined,
            compiled.tableFolds,
         ) === at.rollupAddress
      );
   }
   const buildSource = presentAs(variant.baseBuildSource, {
      name: at.base.name,
      sourceID: at.base.sourceID,
   });
   if (
      computeSourceEntityId(
         buildSource,
         compiled.connectionDigests,
         undefined,
         compiled.tableFolds,
      ) !== at.baseAddress
   ) {
      return false;
   }
   const table = run.manifest.buildManifest.entries[at.baseAddress]?.tableName;
   if (table === undefined) return false;
   const only = new Manifest();
   only.update(at.baseAddress, { tableName: table });
   if (!readsTable(buildSQLOf(variant.source, only.buildManifest), table)) {
      return false;
   }
   return sameColumns(variant.source, planned);
}

/**
 * Whether `sql` reads the table `table` (a manifest's table path) by its whole
 * reference: not as the tail of another schema's path, nor as the head of a
 * longer one.
 */
function readsTable(sql: string, table: string): boolean {
   const escaped = table.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
   return new RegExp(`(?<![\\w.])${escaped}(?![\\w.])`).test(sql);
}

/**
 * Whether two sources output the same columns, by name, type and numeric
 * subtype, in order: an integer column where the plan has a float is drift.
 */
function sameColumns(a: PersistSource, b: PersistSource): boolean {
   return (
      JSON.stringify(outputColumnsOf(a)) === JSON.stringify(outputColumnsOf(b))
   );
}

/**
 * A colocated rollup's entry with `upstreamReuse` saying what its build read: `reused`
 * when it built from its base's stored table ({@link overStoredBaseSource}),
 * absent when it built from the warehouse. Its build source names the base
 * only in the author's text, which the classification reads, so the
 * classification alone would say `reused` either way, and the key checks
 * over the base's rows (checkRollupKeys) would be skipped for a build that
 * grouped exactly those rows. Any other entry is returned as built.
 */
function withRollupUpstreamReuse(
   run: InstructedRun,
   persistSource: PersistSource,
   address: string,
   entry: ManifestEntry,
): ManifestEntry {
   // A `storage=` build reports what it stacked on itself (stackOnParents).
   if (
      run.compiled.tables[persistSource.sourceID]?.kind !== "rollup" ||
      entry.storageDestinationName
   ) {
      return entry;
   }
   const {
      upstreamReuse: _reuse,
      upstreamRecomputeReason: _reason,
      ...rest
   } = entry;
   return run.readFrom?.has(address)
      ? { ...rest, upstreamReuse: "reused" }
      : rest;
}
