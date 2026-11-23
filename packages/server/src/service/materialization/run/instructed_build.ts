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

import type { StringReads } from "../domain/string_reads";
import { upstreamTypesOf } from "../domain/upstream_schemas";
import { tableNamesFoldCase } from "../domain/storage_table_path";
import { errorCodeOf } from "../../../error_code";
import { parseContract } from "../domain/column_contract";
import { assertPairLeafKeysPassed } from "./pair_leaf_gate";
import { instructionTier } from "../domain/placement";
import {
   instructedTable,
   placedLeafKey,
   tableKey,
} from "../domain/physical_table";
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
import { movedSinceBuild } from "./database_column_read";
import type {
   StoredInDestination,
   ParentKeyCheck,
   RollupKeyCheck,
} from "../domain/table_plans";
import type { InlineUniquenessCheck } from "../domain/table_plans";
import {
   assertPlacementNamed,
   assertTierEligible,
   buildSQLOf,
   computeSourceEntityId,
   upstreamSchemasOfSource,
   type RunPlan,
   tierGateInputs,
} from "./run_plan";
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
   ChainedUpstreamMissingError,
   MaterializationEligibilityError,
   SchemaIncompatibleError,
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
import {
   quoteManifestTablePath,
   UnquotableIdentifierError,
} from "../../quoting";
import { DatabaseTier } from "./database_build_strategy";
import {
   StorageBuildStrategy,
   type StoredRollupBase,
} from "./storage_build_strategy";
import {
   storageBuildCasts,
   storedColumnTypeMismatches,
} from "../stored_column_types";
import {
   materializePlansOf,
   sourcesInDependencyOrder,
} from "../domain/compiled_build_plan";
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
   type DatabaseRefusal,
   type InstructedBuildOptions,
   type InstructedRun,
   type PendingRefusal,
   type ResolvedInstruction,
} from "./materialization_instructed_run";
import { freshnessOf } from "../domain/freshness";
import { STORAGE_DIALECT } from "../domain/dialect";
import {
   allSourcesRefusedError,
   refusalRecord,
} from "./materialization_instructed_run";
import {
   BuildEnvironment,
   chainedPlanFacts,
   HostSuppliedWrongTierError,
   manifestExcludingStorage,
   manifestForCompiler,
   upstreamEntryIsCurrent,
   wrongTierDetail,
} from "./build_upstreams";
import {
   isReclaimableStorageTable,
   reportRetainedStorageTables,
} from "./reclaimer";
import { selfAssignTableName } from "./build_table_names";
import { statementRunOptions } from "./build_query_tag";
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
import {
   noteUntypedReference,
   persistedReach,
   readsUntypedReferences,
   type SourceBuildOptions,
} from "./source_build";
import type { KeyCheckRecord } from "./tier";
import {
   buildKindOf,
   buildTierOf,
   reportSourceBuilt,
   reportSourceRefused,
} from "./run_telemetry";

/** The tiers a key check runs through, where its table lives. */
const DATABASE = new DatabaseTier();
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
   leafChecks: readonly PairLeafCheck[],
): Promise<void> {
   const { compiled } = run;
   // A rollup's key checks. Auto-run only; never
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
   // A pair table's leaf key checks ran before the entry was recorded
   // ({@link pairLeafKeysOf}); an auto-run also records them beside the
   // table, for the served copy.
   if (!orchestrated && run.uniqueness) {
      for (const { check, record } of leafChecks) {
         if (!record) continue;
         run.uniqueness.recordRollupKey(sourceEntityId, {
            ...record,
            sourceID: check.sourceID,
         });
      }
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
               "A stored source's primary key check did not run; its queries through a storage #(rollup) rollup read as a join_many are answered live",
            notUnique:
               "A stored source holds its primary key more than once or NULL; its queries through a storage #(rollup) rollup read as a join_many are answered live",
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
   /** The physical table it writes ({@link tableKey}). */
   target: string;
}

/** sourceID -> the sourceIDs it reads, merged across every connection's build. */
function upstreamsBySourceID(builds: RunPlan["builds"]): Map<string, string[]> {
   const out = new Map<string, string[]>();
   for (const build of builds) {
      for (const [sourceID, reads] of Object.entries(build.dependsOn ?? {})) {
         out.set(sourceID, [...(out.get(sourceID) ?? []), ...reads]);
      }
   }
   return out;
}

/**
 * The database table a build wrote, for its ownership mark: undefined for a
 * `storage=` table (a destination's tables are not marked) or a run that
 * names no package to mark it as.
 */
function builtDatabaseTarget(
   run: InstructedRun,
   built: ManifestEntry,
): OwnedTableTarget | undefined {
   if (built.storageDestinationName || !built.connectionName) return undefined;
   if (!runPackageOf(run)) return undefined;
   return {
      physicalTableName: built.physicalTableName,
      connectionName: built.connectionName,
   };
}

/** Mark a table a build wrote as this package's; best-effort. */
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
         code: errorCodeOf(err),
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
    * resolves it on a case-folding engine. Quoted for its own connection's
    * dialect; with no connection named, for the one dialect every connection
    * of this build shares. Undefined, never the name as given, when no dialect
    * is found: the seed is not bound, and a downstream reads its upstream
    * live (or, strict, fails its own compile). Throws
    * {@link UnquotableIdentifierError} for a name its dialect cannot quote.
    */
   private quoteSeedTablePath(
      sourceEntityId: string,
      physicalTableName: string,
      connectionName: string | undefined,
      connections: Map<string, MalloyConnection>,
   ): string | undefined {
      if (connectionName) {
         const connection = connections.get(connectionName);
         if (connection) {
            return quoteManifestTablePath(
               physicalTableName,
               connection.dialectName,
            );
         }
         recordManifestBindDegraded();
         logger.warn(
            "Seeded upstream names a connection not present in this build; " +
               "not binding it for downstream reads",
            { sourceEntityId, connectionName },
         );
         return undefined;
      }
      // An older caller names no connection: the build's own dialect, when
      // its connections agree on one.
      const dialects = new Set(
         [...connections.values()].map((c) => c.dialectName),
      );
      if (dialects.size === 1) {
         return quoteManifestTablePath(physicalTableName, [...dialects][0]);
      }
      logger.warn(
         "Seeded upstream names no connection, and this build's connections " +
            "do not share one dialect to quote it in; not binding it for " +
            "downstream reads",
         { sourceEntityId },
      );
      return undefined;
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
         uniqueness,
         packageRuns,
      } = options;
      const { builds, tables, connectionDigests, connections } = compiled;
      const index = indexInstructions(instructions, tables);
      const run: InstructedRun = {
         compiled,
         environment,
         owner,
         buildMetadata,
         incremental,
         clearedTiers,
         ...(options.freshness ? { freshness: options.freshness } : {}),
         ...(options.onBuildStarted
            ? { onBuildStarted: options.onBuildStarted }
            : {}),
         index,
         // Once per run: it depends only on the plan.
         planFacts: {
            ...chainedPlanFacts(
               tables,
               connectionDigests,
               compiled.tableFolds,
               compiled.upstreamSchemas,
               // A host can imply a destination the annotation does not declare,
               // so the instruction decides a source's current tier.
               (sourceID, sourceEntityId) =>
                  index.bySourceID.get(sourceID) ??
                  index.bySourceEntityId.get(sourceEntityId),
            ),
            hostInstructed,
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
         compiled.upstreamSchemas,
      );

      let settled: ReturnType<typeof settleRefusals> = {
         refusalsInFailures: 0,
         refusedBesideBuilt: {},
      };
      try {
         await this.buildEveryTable(run, builds, tables, connections);
         // Stopped during the last source: the loop has nothing left to
         // check before, so the run settles CANCELLED here rather than
         // committing a partial manifest that would serve.
         if (run.signal?.aborted) throw new BuildCancelledError();

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
         // because a database sibling on its table is refused: the address
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
      // table stands as that check found it.
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
                  code: errorCodeOf(retireErr),
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
    * Build every planned table in the plan's order: each after every table
    * it reads (a rollup after its base, through the plan's edges), so an
    * upstream this run builds is there to read.
    */
   private async buildEveryTable(
      run: InstructedRun,
      builds: RunPlan["builds"],
      tables: RunPlan["tables"],
      connections: RunPlan["connections"],
   ): Promise<void> {
      for (const build of builds) {
         if (!connections.get(build.connectionName)) {
            throw new BadRequestError(
               `Connection '${build.connectionName}' not found`,
            );
         }
      }
      // Each source after every source it reads, across builds: a
      // downstream's edges (a persisted view, query or rollup) may name a
      // source another build orders, and the instructions' own order is
      // never read.
      for (const { sourceID, connectionName } of sourcesInDependencyOrder(
         builds,
      )) {
         const table = tables[sourceID];
         if (!table) continue;
         const persistSource = table.source;
         const connection = connections.get(connectionName)!;
         await this.buildInRun(run, persistSource, connectionName, connection);
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
      connections: RunPlan["connections"],
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
            let tableName: string | undefined;
            try {
               tableName = this.quoteSeedTablePath(
                  sourceEntityId,
                  entry.physicalTableName,
                  entry.connectionName,
                  connections,
               );
            } catch (err) {
               if (!(err instanceof UnquotableIdentifierError)) throw err;
               // A name its dialect cannot quote is not bound: a downstream
               // reads its upstream live (or, strict, fails its own compile),
               // and the rest of the run goes on.
               logger.warn(
                  "Seeded upstream's table name cannot be quoted for its dialect; " +
                     "not binding it for downstream reads",
                  { sourceEntityId, code: errorCodeOf(err) },
               );
            }
            if (tableName !== undefined) {
               manifest.update(sourceEntityId, { tableName });
            }
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
      // Set by the `storage=` build when it grouped the base's stored table.
      let stacked = false;
      const overStored = overStoredUpstreamSource(run, persistSource);
      // A rollup the plan built over its base's table, over the table this
      // run holds for it: refused, as the source's own failure, when there
      // is none it can read.
      let offered: StoredRollupBase | undefined;
      let overBase: PersistSource | undefined;
      try {
         offered = storedRollupBase(run, persistSource, gated);
         overBase =
            overStored === undefined
               ? await overStoredBaseSource(
                    run,
                    persistSource,
                    gated,
                    connection,
                 )
               : undefined;
      } catch (err) {
         recordGatedSourceFailure(run, persistSource, gated, err, undefined);
         return;
      }
      const storedBase = offered && {
         ...offered,
         onStacked: () => {
            stacked = true;
         },
      };
      const built = await this.buildGatedSource(
         run,
         overStored ?? overBase ?? persistSource,
         graphConnectionName,
         connection,
         gated,
         storedBase,
         overStored !== undefined,
      );
      if (!built) return;
      // A `storage=` rollup that grouped its base's stored table is anchored
      // at that table, as a database one is.
      if (storedBase && stacked) {
         (run.readFrom ??= new Map()).set(sourceEntityId, [storedBase.address]);
      }
      const leafChecks = await runPairLeafKeyChecks(
         pairLeafKeyChecksOf(
            compiled.tables[persistSource.sourceID]?.materialize,
         ),
         connection,
         persistSource.name,
         writtenTableIdentity(instruction, graphConnectionName),
      );
      const entry = withLeafKeys(
         withBuiltProvenance(
            run,
            sourceEntityId,
            withRollupUpstreamReuse(run, persistSource, sourceEntityId, built),
         ),
         leafChecks,
      );
      run.builtSources.push(persistSource.name);
      reportSourceBuilt(
         {
            kind: buildKindOf(compiled, persistSource.sourceID),
            tier: entry.storageDestinationName ? "storage" : "database",
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
         leafChecks,
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
    * eligibility (its tier's gate), a database sibling's refusal, and
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
         compiled.upstreamSchemas,
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
            compiled.upstreamSchemas,
         );

      // A host-supplied name its dialect cannot quote (it does not parse, or
      // holds a part the dialect cannot hold) fails the same way on every
      // attempt, so this source alone is refused, not built and retried.
      const unquotable = unquotableTargetName(persistSource, instruction);
      if (unquotable) {
         recordPendingRefusal(
            run,
            persistSource,
            instruction,
            sourceEntityId,
            new MaterializationEligibilityError({
               message:
                  `'${persistSource.name}' is not built: its table name ` +
                  `cannot be written as SQL. ${unquotable}`,
            }),
            true,
         );
         return undefined;
      }

      // A database table is served by its address, so the bind refuses the
      // whole address when any source on it is refused
      // (collectDatabaseSourceEligibility). Building it would write a table
      // nothing serves; refuse this source with its sibling's reason instead,
      // so every source on the address gets one outcome whatever the order.
      if (!instruction.destination) {
         const sibling = databaseRefusalOn(
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
                     `gate refuses for a database build, and a database ` +
                     `table is served to every source on it. ` +
                     sibling.message,
               }),
               false,
            );
            run.heldBackBySibling = true;
            return undefined;
         }
      }

      // One physical table, written once. A base and its extension (`#@
      // persist` is inherited) compile to the same address, and a source used
      // across models appears in several builds. Only the same-content case is
      // decided here; DIFFERENT definitions on one table were reported (and
      // refused unless PERSIST_COLLISION_ENFORCE=false) by
      // assertNoTableCollisions, so reaching here means the deployment opted
      // out, to warn and write anyway.
      const target = tableKey(
         instructedTable(instruction, graphConnectionName),
         tableNamesFoldCase(
            persistSource.dialectName,
            !!instruction.destination,
         ),
      );
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
      storedBase?: StoredRollupBase,
      /** `persistSource` is a view or query compiled over its upstreams' tables. */
      overStoredUpstreams = false,
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

         // A view or query over a pair table relies on the leaf key check
         // its upstream's table recorded: refused before anything is
         // written when it failed or is not recorded (pair_leaf_gate.ts).
         if (overStoredUpstreams) {
            assertPairLeafKeysOfUpstream(run, persistSource);
         }

         // An inlined join_one must match at most one row: checked on
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
         run.onBuildStarted?.(instruction, ownedTarget !== undefined);
         let built = await this.seams.buildOneSource({
            persistSource,
            instruction,
            connection,
            manifest: run.manifest,
            environment: run.environment,
            builtEntries: run.entries,
            builtThisRun: run.builtAddresses,
            buildMetadata: run.buildMetadata,
            incremental: run.incremental,
            // The CONTENT address, distinct from the instruction's
            // caller-assigned identity: the ledger is keyed by it so a
            // boundary can never be read against different SQL.
            contentSourceEntityId: sourceEntityId,
            planFacts: run.planFacts,
            ...(replacesView ? { replacesView } : {}),
            ...(storedBase ? { storedBase } : {}),
            ...(overStoredUpstreams ? { overStoredUpstreams } : {}),
            ...storedInDestinationOf(compiled.tables[persistSource.sourceID]),
            ...stringReadsOf(compiled.tables[persistSource.sourceID]),
            ...upstreamTypesOption(compiled, persistSource.sourceID),
            ...(compiled.tables[persistSource.sourceID]?.kind === "rollup"
               ? { rollup: true }
               : {}),
            ...(run.signal ? { signal: run.signal } : {}),
         });
         // A manifest travels without its build plan, so this is the only
         // signal that a table belongs to a rollup in no model file; served
         // as an ordinary binding it would be silently dropped (see
         // preaggregation_serve_bindings).
         if (compiled.tables[persistSource.sourceID]?.rollup) {
            built = { ...built, origin: "rollup" as const };
         }
         // The upstream tables' column types its address folds in, on both
         // tiers, so a host keeps what each table was built from.
         const upstreamSchemas = upstreamSchemasOfSource(
            persistSource,
            compiled.upstreamSchemas,
         );
         if (upstreamSchemas) built = { ...built, upstreamSchemas };
         // Every database table a build writes carries the package's mark,
         // whoever named it (an author's name=, Publisher, or a host's
         // instruction), so a caller's drop can prove the table is this
         // package's once its run's record is gone. Best-effort, after the
         // build: it never changes what was built or what is served.
         const markTarget = ownedTarget ?? builtDatabaseTarget(run, built);
         if (markTarget) {
            await markBuiltOwnedTable(
               run,
               persistSource,
               markTarget,
               connection,
            );
         }
         return built;
      };
      try {
         // An instructed destination that cannot be honored REFUSES, never
         // falls through to a database build: that would write the caller's
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
      const { instruction } = resolved;
      // Auto-run refused it and reported it already; it reaches the loop only
      // through an eligible sibling's instruction on the same address.
      if (run.priorRefusedIDs.has(persistSource.sourceID)) return true;
      // A refusal fails THIS source, not the run (as in
      // deriveSelfInstructions), and is reported in `failures` because an
      // absent entry reads as "built". A host building from the plan never
      // instructs a refused source, so reaching the catch means the plan and
      // this gate disagreed.
      try {
         // Every instruction into storage, however it was found, takes the
         // storage gate the plan uses, so no path into a storage build is
         // ungated; host-supplied ones are gated too, not trusted. Skipped
         // when the mode is off: the destination refusal owns that.
         const storage =
            instructionTier(instruction, getPersistStorageMode()) === "storage";
         // Every other instruction (database, or any build while the mode is
         // off) takes the database gate, unless auto-run already cleared the
         // source for this tier. Never both: the storage gate already runs
         // the same IR walk.
         // Its own placement, never a sibling's (assertPlacementNamed).
         assertPlacementNamed(persistSource);
         const cleared = run.clearedTiers?.get(persistSource.sourceID);
         if (storage ? cleared !== "storage" : cleared === undefined) {
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
            run.compiled.upstreamSchemas,
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
function failedRunError<
   E extends BuildRunFailedError | MaterializationEligibilityError,
>(run: InstructedRun, err: E): E {
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
 * Why `instruction`'s table name cannot be quoted for the dialect it is
 * written in (the source's own, or the storage destination's), or undefined
 * when it can.
 */
function unquotableTargetName(
   persistSource: PersistSource,
   instruction: BuildInstruction,
): string | undefined {
   try {
      quoteManifestTablePath(
         instruction.physicalTableName,
         instruction.destination ? STORAGE_DIALECT : persistSource.dialectName,
      );
      return undefined;
   } catch (err) {
      if (err instanceof UnquotableIdentifierError) return err.message;
      throw err;
   }
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
         ...(err.reason ? { refusalReason: err.reason } : {}),
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
         instruction.destination ? "storage" : "database",
         run.compiled.tables[persistSource.sourceID]?.modelPath,
         run.compiled.tables[persistSource.sourceID]?.kind,
      ),
      sourceID: persistSource.sourceID,
      own,
   });
}

/**
 * Another planned source on `address` that the database gate refuses, or
 * undefined. Only the sources on that address are gated, once per address per
 * run, so an unrelated source can never fail this build. Any error from the
 * gate refuses, as the bind's collectDatabaseSourceEligibility treats it, so
 * build and serve agree on the address. The first refused source in sourceID
 * order is named, so the reason never depends on run order.
 */
function databaseRefusalOn(
   run: InstructedRun,
   address: string,
   sourceID: string,
): DatabaseRefusal | undefined {
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
               compiled.upstreamSchemas,
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
   run.databaseRefusalsByAddress ??= new Map();
   let refusals = run.databaseRefusalsByAddress.get(address);
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
                  "The database gate could not decide a source; refusing its address, as the bind does",
                  {
                     packageName: run.owner?.packageName,
                     sourceName: source.name,
                     code: errorCodeOf(err),
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
      run.databaseRefusalsByAddress.set(address, refusals);
   }
   return refusals.find((refusal) => refusal.sourceID !== sourceID);
}

/** The table an instructed build writes, as {@link tableIdentityKey} names it. */
function writtenTableIdentity(
   instruction: BuildInstruction,
   connectionName: string,
): string {
   return placedLeafKey(instructedTable(instruction, connectionName));
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
         compiled.upstreamSchemas,
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
      // Each also under the compiler's own keys, which it looks tables up
      // by; a key the parent reads holds only its own upstream's table.
      const { reachedIds } = persistedReach(
         parent,
         run.environment,
         run.planFacts,
      );
      const withTable = manifestForCompiler(
         { ...reduced, strict: false },
         run.planFacts,
         reachedIds,
      );
      const withoutTable = manifestForCompiler(
         {
            ...reduced,
            strict: false,
            entries: Object.fromEntries(
               Object.entries(reduced.entries).filter(([id]) => id !== address),
            ),
         },
         run.planFacts,
         reachedIds,
      );
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
         { sourceName: parent.name, join: check.join, code: errorCodeOf(err) },
      );
      return undefined;
   }
}

/**
 * The key checks of a rollup just built, recorded against the table written:
 * its own (a join_one keyed on its grain) and its base's
 * primary key, both over the base's rows on the source's own connection, the
 * rows a build from the author's definition groups; and the primary key of
 * each query source that reads a database rollup through a join_many on it.
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
   compiled: RunPlan,
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
         "A #(rollup) rollup was built over a stored upstream, so its key check over the base's rows is not run; queries through its join_one that need the key unique are answered live",
         { rollup: persistSource.name, keyColumn: own.keyColumn },
      );
   }
   if (own && groupsBaseRows) {
      await checkAndRecordKey(
         ledger,
         address,
         "rollup",
         () => DATABASE.checkKey({ connection }, own, table),
         {
            notRun:
               "A #(rollup) rollup's key check did not run; queries through its join_one that need the key unique are answered live",
            notUnique:
               "A #(rollup) rollup's base holds its join_one key more than once; queries that read that join_one beside other aggregates are answered live",
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

/** The key check a run builds for the rollup `sourceID`, if any. */
function rollupKeyCheckOf(
   compiled: RunPlan,
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
   compiled: RunPlan,
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
         () => DATABASE.checkKey({ connection }, check, table),
         {
            notRun:
               "A query source's primary key check did not run; its queries through a #(rollup) rollup read as a join_many are answered live",
            notUnique:
               "A query source holds its primary key more than once or NULL; its queries through a #(rollup) rollup read as a join_many are answered live",
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

/** One leaf key check of a built pair table, and what it found (undefined: it did not run). */
interface PairLeafCheck {
   check: ReturnType<typeof pairLeafKeyChecksOf>[number];
   record: KeyCheckRecord | undefined;
}

/**
 * Run each leaf key check of a built pair table on the source's connection,
 * on every build (auto-run or a host's): the leaf's key unique and never
 * NULL over the leaf source's rows. What each found is recorded in the
 * table's contract (`ColumnContract.keys`), which a view built over the
 * pair table needs passed. A check that does not run records nothing.
 * Never fails the build.
 */
async function runPairLeafKeyChecks(
   checks: ReturnType<typeof pairLeafKeyChecksOf>,
   connection: MalloyConnection,
   sourceName: string,
   table: string,
): Promise<PairLeafCheck[]> {
   const out: PairLeafCheck[] = [];
   for (const check of checks) {
      const logContext = { source: sourceName, keyColumn: check.keyColumn };
      let record: KeyCheckRecord | undefined;
      try {
         record = await DATABASE.checkKey({ connection }, check, table);
      } catch (err) {
         logger.warn(
            "A pair table's leaf key check did not run; counts through its inlined join_many are answered live, and a view over it is not built",
            { ...logContext, code: errorCodeOf(err) },
         );
      }
      recordKeyCheck(
         "pair_leaf",
         !record ? "not_run" : record.unique ? "unique" : "not_unique",
      );
      if (record && !record.unique) {
         logger.warn(
            "An inlined join_many's joined source holds its primary key more than once or NULL; counts through it are answered live, and a view over it is not built",
            logContext,
         );
      }
      out.push({ check, record });
   }
   return out;
}

/** `entry` with its leaf key checks in its contract's `keys`. */
function withLeafKeys(
   entry: ManifestEntry,
   leafChecks: readonly PairLeafCheck[],
): ManifestEntry {
   const contract = parseContract(entry.contract);
   if (!contract || leafChecks.length === 0) return entry;
   const keys = { ...(contract.keys ?? {}) };
   for (const { check, record } of leafChecks) {
      if (record) {
         keys[check.contractKey] = { unique: record.unique, level: "leaf" };
      }
   }
   return { ...entry, contract: { ...contract, keys } };
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
      logger.warn(messages.notRun, { ...logContext, code: errorCodeOf(err) });
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
         compiled.upstreamSchemas,
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
         compiled.upstreamSchemas,
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
         compiled.upstreamSchemas,
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
 * Why a rollup the plan built over its base's table cannot read the table
 * the run holds for it, or undefined when it can: none is built in this run
 * or carried into it, it is at another tier or destination than the base
 * declares now, or, with a declared window, it is of unknown age or past it
 * ({@link baseFreshEnough}). `destination`: a `storage=` rollup's, whose
 * base's table must be in it, with the column list its build reads it by.
 * A host's reference at the wrong tier is refused as the host's
 * (HostSuppliedWrongTierError).
 */
function rollupBaseUnusable(
   run: InstructedRun,
   gated: Pick<GatedSource, "sourceEntityId">,
   names: { sourceName: string; baseName: string },
   baseAddress: string,
   baseEntry: ManifestEntry | undefined,
   destination?: string,
): string | undefined {
   const { sourceName, baseName } = names;
   if (!baseEntry?.physicalTableName) {
      return "no table of it is built in this run or carried into it";
   }
   if (
      (baseEntry.storageDestinationName ?? undefined) !== destination ||
      !upstreamEntryIsCurrent(baseAddress, baseEntry, run.planFacts)
   ) {
      // A host's reference at the wrong tier: the host must change it.
      if (
         run.planFacts.hostInstructed &&
         !run.builtAddresses.has(baseAddress)
      ) {
         throw new HostSuppliedWrongTierError(sourceName, [
            {
               name: baseName,
               detail: wrongTierDetail(
                  baseName,
                  baseAddress,
                  baseEntry,
                  run.planFacts,
               ),
            },
         ]);
      }
      return (
         `its table ${baseEntry.physicalTableName} is ` +
         (baseEntry.storageDestinationName
            ? `in destination '${baseEntry.storageDestinationName}'`
            : `in its warehouse`) +
         `, not where the plan builds over it`
      );
   }
   if (destination !== undefined && !baseEntry.schema?.length) {
      return `its table ${baseEntry.physicalTableName} carries no column list`;
   }
   if (!baseFreshEnough(run, gated.sourceEntityId, baseAddress, baseEntry)) {
      return (
         `its table ${baseEntry.physicalTableName} is of unknown age or ` +
         `past the rollup's freshness window`
      );
   }
   return undefined;
}

/**
 * The refusal of a rollup the plan built over its base's table, when the
 * run holds no table of the base it can read: a missing upstream, which
 * inherits the base's own failure when the base failed in this run
 * (classifySourceFailure).
 */
function refusedRollupBase(
   sourceName: string,
   baseName: string,
   detail: string,
): ChainedUpstreamMissingError {
   return new ChainedUpstreamMissingError(
      [baseName],
      `persisted upstream of '${sourceName}': '${baseName}' — ${detail}; ` +
         `a rollup is built over its base's table or not at all, never by ` +
         `recomputing its base from raw`,
      [{ name: baseName, detail }],
   );
}

/**
 * The source a persisted view or named query builds from when the plan
 * decided it builds over its upstreams' warehouse tables: its build source
 * compiled over them, proven at plan time to build the planned table
 * (over_stored_proof.ts). In either tier its build runs in the warehouse and
 * reads each upstream's table through the build manifest as any chained
 * build does (a `storage=` one then copies the result into its
 * destination), so a moved, missing or wrong-tier upstream takes the
 * ordinary chained path, which refuses it. Undefined when the plan did not
 * decide so: it builds from its definition.
 */
function overStoredUpstreamSource(
   run: InstructedRun,
   persistSource: PersistSource,
): PersistSource | undefined {
   const table = run.compiled.tables[persistSource.sourceID];
   if (table?.kind !== "view" || !table.overStoredBase) return undefined;
   return table.overStoredBase.source;
}

/**
 * Refuse a view or query planned over a pair table with its join put back
 * (`OverStoredBase.pairLeafKeys`) unless the upstream's current entry in the
 * run records each leaf key check it relies on as passed
 * ({@link assertPairLeafKeysPassed}). A view over any other table passes.
 */
function assertPairLeafKeysOfUpstream(
   run: InstructedRun,
   persistSource: PersistSource,
): void {
   const { compiled } = run;
   const table = compiled.tables[persistSource.sourceID];
   if (table?.kind !== "view" || !table.overStoredBase?.pairLeafKeys) return;
   const { pairLeafKeys, rerootedSourceID } = table.overStoredBase;
   const upstream =
      rerootedSourceID === undefined
         ? undefined
         : compiled.tables[rerootedSourceID];
   let entry: ManifestEntry | undefined;
   if (upstream) {
      try {
         entry =
            run.entries[
               computeSourceEntityId(
                  upstream.source,
                  compiled.connectionDigests,
                  upstream.target,
                  compiled.tableFolds,
                  compiled.upstreamSchemas,
               )
            ];
      } catch {
         entry = undefined;
      }
   }
   assertPairLeafKeysPassed(
      persistSource.name,
      upstream?.source.name ?? rerootedSourceID ?? "its upstream",
      pairLeafKeys,
      entry,
   );
}

/**
 * The source a rollup builds from when this run holds its base's
 * table: the rollup compiled over that table (rollup_compile.ts), so its
 * build reads the table rather than the warehouse, through the build
 * manifest as any chained build does. Only when every one of these holds,
 * otherwise undefined and the rollup builds from the warehouse as planned:
 *
 *  - its base is a database `#@ persist` source or a database
 *    `#(materialize)` plain table (the plan offers the compile only then),
 *    in either tier: a `storage=` rollup's build runs in the warehouse too;
 *  - the run's entries hold the base's table at the address the plan gives
 *    it, built this run or carried under the reuse rules, database and at
 *    the tier the base declares now. The database gate refused nothing on
 *    that address, or it would not be built or carried, and a table's rows
 *    do not depend on any gate: each is applied when a query reads it;
 *  - with a declared window, the base's table is new enough
 *    ({@link baseFreshEnough}), whoever instructed the run;
 *  - a table this run did not build holds the columns its build recorded
 *    (`movedSinceBuild`);
 *  - the plan proved its compile over the base's table builds the planned
 *    table (over_stored_proof.ts), and recorded it (`overStoredBase`).
 *
 * Records the base as what the rollup read, for its freshness anchor.
 */
async function overStoredBaseSource(
   run: InstructedRun,
   persistSource: PersistSource,
   gated: Pick<GatedSource, "instruction" | "sourceEntityId">,
   /** The rollup's connection, which its build reads the base's table on. */
   connection: MalloyConnection,
): Promise<PersistSource | undefined> {
   const { compiled } = run;
   const table = compiled.tables[persistSource.sourceID];
   if (table?.kind !== "rollup" || !table.overStoredBase) return undefined;
   // The table it reads: its base's, or that of the `#(materialize)` source
   // its base extends, which its compile re-roots.
   const baseID =
      table.overStoredBase.rerootedSourceID ?? table.rollup.baseSourceID;
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
         compiled.upstreamSchemas,
      );
   } catch {
      return undefined;
   }
   const baseName = base.source.name;
   const baseEntry = run.entries[baseAddress];
   const unusable = rollupBaseUnusable(
      run,
      gated,
      { sourceName: persistSource.name, baseName },
      baseAddress,
      baseEntry,
   );
   if (unusable !== undefined) {
      throw refusedRollupBase(persistSource.name, baseName, unusable);
   }
   // A table altered in its warehouse since its build (a column retyped or
   // dropped) is not the base's rows: refused, and the table handed to the
   // drift handler, which reads it again; a standalone run then rebuilds it.
   const move = run.builtAddresses.has(baseAddress)
      ? undefined
      : (
           await movedSinceBuild(
              new Map([[baseAddress, baseEntry!]]),
              connection,
           )
        ).get(baseAddress);
   if (
      move?.kind === "types_unrecorded" &&
      readsUntypedReferences(run.manifest, run.planFacts)
   ) {
      noteUntypedReference(persistSource.name, baseName, "rollup");
   } else if (move) {
      run.environment.reportIncompatible?.([baseAddress]);
      throw new SchemaIncompatibleError(
         `'${persistSource.name}' was not built: persisted upstream ` +
            `'${baseName}' was not read from its table, since ${move.reason}, ` +
            `and a rollup is never built by recomputing its base from raw. ` +
            `The package reads its tables and derives its plan again.`,
         [baseAddress],
      );
   }
   (run.readFrom ??= new Map()).set(gated.sourceEntityId, [baseAddress]);
   return table.overStoredBase.source;
}

/**
 * The base a `storage=` rollup may build over in its destination
 * (storage_build_strategy.ts), or undefined, and the rollup builds from the
 * warehouse as planned. Only when every one of these holds:
 *
 *  - the plan offers it: a `storage=` rollup over a base whose own
 *    `#@ persist`, or `#(materialize)` planned as a plain table, stores it
 *    in the same destination, with no gate (rollup_compile.ts);
 *  - the run's entries hold the base's table at the address the plan gives
 *    it, built this run or carried, in that destination, at the tier the
 *    base declares now, with the column list its build reads it by. A
 *    rollup over a gated base is never planned there: it builds from the
 *    warehouse (rollup_compile.ts's `baseGated` guard);
 *  - with a declared window, the base's table is new enough
 *    ({@link baseFreshEnough}), whoever instructed the run.
 *
 * The build keeps the table only when it has the columns, and their types,
 * the planned build writes; the address is the planned one either way.
 */
function storedRollupBase(
   run: InstructedRun,
   persistSource: PersistSource,
   gated: Pick<GatedSource, "instruction" | "sourceEntityId">,
): StoredRollupBase | undefined {
   const { compiled } = run;
   const table = compiled.tables[persistSource.sourceID];
   if (table?.kind !== "rollup" || !table.storedBase) return undefined;
   const destination = gated.instruction.destination;
   if (!destination) return undefined;
   const base = compiled.tables[table.storedBase.baseSourceID];
   // A `#@ persist` base, or a `#(materialize)` base's plain table (a pair
   // table holds a row per pair, never the base's rows).
   if (!base || (base.kind !== "persist" && base.kind !== "materialize")) {
      return undefined;
   }
   let address: string;
   try {
      address = computeSourceEntityId(
         base.source,
         compiled.connectionDigests,
         base.target,
         compiled.tableFolds,
         compiled.upstreamSchemas,
      );
   } catch {
      return undefined;
   }
   const entry = run.entries[address];
   const unusable = rollupBaseUnusable(
      run,
      gated,
      { sourceName: persistSource.name, baseName: base.source.name },
      address,
      entry,
      destination,
   );
   if (unusable !== undefined) {
      throw refusedRollupBase(persistSource.name, base.source.name, unusable);
   }
   // The table must hold every column the base's current build writes, each
   // of a physical type that serves as its declared type: a carried table
   // whose types drifted is not grouped as if it were the base, and is
   // handed to the drift handler.
   const declared = storageBuildCasts(base.source).declaredColumns;
   const stored = entry!.schema!.flatMap((c) =>
      c.name && c.type ? [{ name: c.name, type: c.type }] : [],
   );
   const storedNames = new Set(stored.map((c) => c.name));
   const gone = declared.filter((c) => !storedNames.has(c.name));
   const retyped = storedColumnTypeMismatches(stored, declared);
   if (gone.length > 0 || retyped.length > 0) {
      run.environment.reportIncompatible?.([address]);
      throw new SchemaIncompatibleError(
         `'${persistSource.name}' was not built over its base ` +
            `'${base.source.name}''s table ${entry!.physicalTableName} in ` +
            `'${destination}': ` +
            [
               ...gone.map((c) => `it holds no column '${c.name}'`),
               ...retyped.map(
                  (m) =>
                     `column '${m.column}' is ${m.storedType}, not ${m.declaredType}`,
               ),
            ].join(", ") +
            `. A rollup is never built by recomputing its base from raw; ` +
            `the package reads its tables and derives its plan again.`,
         [address],
      );
   }
   return {
      baseSourceID: table.storedBase.baseSourceID,
      baseName: base.source.name,
      address,
      entry: entry!,
      companion: table.storedBase.companion,
      grain: table.rollup.grainDimensions,
      measures: table.rollup.measures.map((m) => m.name),
   };
}

/**
 * A built entry with its provenance: `builtAt`, now, and in `builtFrom` each
 * stored table a build read that the database build did not already name
 * (a `storage=` rollup that grouped its base's table, `readFrom`).
 */
function withBuiltProvenance(
   run: InstructedRun,
   address: string,
   entry: ManifestEntry,
): ManifestEntry {
   const builtFrom = [...(entry.builtFrom ?? [])];
   for (const upstream of run.readFrom?.get(address) ?? []) {
      if (builtFrom.some((b) => b.sourceEntityId === upstream)) continue;
      const read = run.entries[upstream];
      if (!read?.physicalTableName) continue;
      builtFrom.push({
         sourceEntityId: upstream,
         physicalTableName: read.physicalTableName,
         ...(read.storageDestinationName
            ? { storageDestinationName: read.storageDestinationName }
            : {}),
         ...(read.builtAt ? { builtAt: read.builtAt } : {}),
      });
   }
   const { builtFrom: _unset, ...rest } = entry;
   return {
      ...rest,
      builtAt: new Date().toISOString(),
      ...(builtFrom.length > 0 ? { builtFrom } : {}),
   };
}

/**
 * A rollup's entry with `upstreamReuse` saying what its build read: `reused`
 * when it built from its base's stored table ({@link overStoredBaseSource}),
 * and absent when the plan built it from the warehouse. Its build source
 * names the base only in the author's text, which the classification reads,
 * so the classification alone would say `reused` either way, and the key
 * checks over the base's rows (checkRollupKeys) would be skipped for a build
 * that grouped exactly those rows. Any other entry is returned as built.
 */
function withRollupUpstreamReuse(
   run: InstructedRun,
   persistSource: PersistSource,
   address: string,
   entry: ManifestEntry,
): ManifestEntry {
   // A `storage=` build reports what it read itself (`reused` when it
   // stacked on a stored table, storage_build_strategy.ts), unless it read
   // its database base's table in the warehouse, as a database rollup does.
   if (
      run.compiled.tables[persistSource.sourceID]?.kind !== "rollup" ||
      (entry.storageDestinationName && !run.readFrom?.has(address))
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

/**
 * The strings a `storage=` build over stored tables compares, as its plan
 * recorded them, as a build option.
 */
function stringReadsOf(
   table: InstructedRun["compiled"]["tables"][string] | undefined,
): { stringReads?: StringReads } {
   return table?.stringReads ? { stringReads: table.stringReads } : {};
}

/**
 * The warehouse type of each column the table of `sourceID` stores as a plain
 * copy of one warehouse column (its plan's `columnOrigins`), from the plan's
 * own upstream readings, as a build option; absent when none is known.
 */
function upstreamTypesOption(
   compiled: RunPlan,
   sourceID: string,
): {
   upstreamTypes?: ReadonlyMap<string, { dialect: string; nativeType: string }>;
} {
   const upstreamTypes = upstreamTypesOf(
      compiled.tables[sourceID]?.columnOrigins,
      compiled.upstreamSchemas,
   );
   return upstreamTypes ? { upstreamTypes } : {};
}

/** A view's or query's proven build in its destination, as a build option. */
function storedInDestinationOf(
   table: InstructedRun["compiled"]["tables"][string] | undefined,
): { storedInDestination?: StoredInDestination } {
   return table?.kind === "view" && table.storedInDestination
      ? { storedInDestination: table.storedInDestination }
      : {};
}
