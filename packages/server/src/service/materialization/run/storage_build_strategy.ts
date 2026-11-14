// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Manifest, PersistSource } from "@malloydata/malloy";
import {
   ChainedShapeNotCarriedError,
   ChainedUpstreamMissingError,
   MaterializationEligibilityError,
   WarehouseRecomputeRefusedError,
} from "../../../errors";
import { logger } from "../../../logger";
import { errorCodeOf } from "./error_code";
import {
   recordStoredColumnTypeMismatch,
   recordChainedStorageBuild,
} from "../../../materialization_metrics";
import { errMessage } from "../../../utils";
import type { ApiConnection } from "../../model";
import type { QueryMetadata } from "../../query_metadata";
import { withStatementUsage } from "../../statement_usage";
import {
   buildVirtualMap,
   bindingServesModelSource,
   deriveServeBindings,
   missingPersistedTables,
   reportLeftOutColumns,
   type ServeBinding,
} from "../serve/materialization_serve_transform";
import { authorModelLiftContext } from "../text/author_model";
import { declarableColumns } from "../stored_column_types";
import { reachPersisted } from "../text/source_map";
import {
   extensionOverStoredModel,
   type MaterializeParent,
   type MaterializePlan,
   type StoredChain,
} from "../plan/build_file_generator";
import {
   STORED_COLUMN_TYPE_MISMATCH_CODE,
   storageBuildCasts,
   storedColumnTypeMismatches,
} from "../stored_column_types";
import { BuildInstruction, ManifestEntry } from "../domain/types";
import type { BuildStrategy } from "./build_strategy";
import {
   ledgerFields,
   refreshFields,
   reportIncrementalStep,
   resetLedger,
   sourceIncrementalRefresh,
   storageRefreshFor,
   type IncrementalRunContext,
} from "./incremental_build";
import {
   BilledReadNotCapturedError,
   buildDownstreamIntoStorage,
   buildSourceIntoStorage,
   oneFileModel,
   type StorageBuildResult,
   type StorageIncrementalRefresh,
} from "./materialization_build_session";
import {
   BuildEnvironment,
   ChainedPlanFacts,
   NO_PLAN_FACTS,
   partitionColumnsForBuild,
   readAuthorFile,
   redactConnectionSecrets,
   currentUpstreamEntries,
   noteWrongTierRecompute,
   HostSuppliedWrongTierError,
   upstreamEntryIsCurrent,
   WRONG_TIER_MARKER,
   wrongTierDetail,
} from "./materialization_run_support";
import { carriesAuthorSql, isSameCatalog } from "./same_catalog_build";
import {
   destinationSchemaOf,
   storageTablePath,
} from "../domain/storage_table_path";

/** What a `storage=` build of one source starts from. */
export interface StorageBuildRequest {
   persistSource: PersistSource;
   instruction: BuildInstruction;
   manifest: Manifest;
   environment: BuildEnvironment;
   /** The public-column projection of the build SQL, which the CTAS reads. */
   publicBuildSQL: string;
   /** The same SQL unprojected, which a delta wraps in its range predicate. */
   buildSQL: string;
   builtEntries: Record<string, ManifestEntry>;
   dependsOnStorageUpstream: boolean;
   /** Applied by the passthrough itself (see {@link buildSourceIntoStorage}). */
   queryMetadata?: QueryMetadata;
   /** The package being built, for the build's statement_usage record. */
   packageName?: string;
   /** Present when any source in the run declared an incremental refresh. */
   incremental?: IncrementalRunContext;
   /** The source's CONTENT address — see buildOneSource's parameter of the same name. */
   contentSourceEntityId?: string;
   /** The plan's stored-table facts — see buildOneSource's parameter of the same name. */
   planFacts?: ChainedPlanFacts;
   /** The author-model lift context `buildOneSource` built for this source. */
   lift?: ReturnType<typeof authorModelLiftContext>;
   /**
    * The `upstreamReuse` fields for a build that does NOT stack on a parent
    * ({@link upstreamReuseFromManifest}).
    */
   baseReuse?: Pick<ManifestEntry, "upstreamReuse" | "upstreamRecomputeReason">;
   /**
    * The name holds this package's own `name=` view, which the table replaces
    * in the transaction that creates it (see viewToTableSwap, name_view.ts).
    */
   replacesNameView?: boolean;
}

/**
 * The storage tier: materialize a source into a `storage=` destination
 * (DuckDB/DuckLake), never the source or serve connection. A source that
 * reads a stored upstream stacks on that parent's table when it can, and
 * recomputes from the source warehouse otherwise; a delta of an incremental
 * table applies inside the destination session. Whether a stored table can
 * be read is decided at bind, per source, by the served copy's stored-type
 * plan: a table whose columns no type declares faithfully is built and
 * answers live.
 */
export class StorageBuildStrategy
   implements BuildStrategy<StorageBuildRequest>
{
   constructor(
      private readonly seams: {
         /**
          * Stack a chained source on its parents. The facade routes this back
          * through its own `buildDownstreamViaParents` so its specs can
          * substitute it.
          */
         buildDownstreamViaParents: StorageBuildStrategy["buildDownstreamViaParents"];
      },
   ) {}

   /**
    * Materialize a source into a `storage=` destination (DuckDB/DuckLake) via a
    * query-passthrough CTAS on a build-scoped session. The entry records the
    * captured DuckDB schema for the serve transform; `connectionName` names the
    * SOURCE warehouse, `storageDestinationName` where the table lives.
    */
   async build(params: StorageBuildRequest): Promise<ManifestEntry> {
      const {
         persistSource,
         instruction,
         manifest,
         environment,
         builtEntries,
         dependsOnStorageUpstream,
         queryMetadata,
         incremental,
         planFacts = NO_PLAN_FACTS,
         baseReuse = {},
         lift = authorModelLiftContext(
            params.persistSource._model?._modelDef,
            readAuthorFile,
         ),
      } = params;
      const sourceEntityId = instruction.sourceEntityId;
      const physicalTableName = instruction.physicalTableName;
      const destinationName = instruction.destination!;
      const sourceConnection = environment.getApiConnection(
         persistSource.connectionName,
      );
      // From the destination list, never the connection list: an unconfigured
      // destination must fail (422) rather than resolve a same-named connection
      // and write a tenant's own warehouse.
      const destinationConnection =
         environment.getStorageDestination(destinationName);

      // Incremental refresh of a STORED table: the warehouse computes the delta,
      // the destination engine applies it; seed-vs-delta is the shared planner's.
      const incrementalRefresh = sourceIncrementalRefresh(
         incremental,
         persistSource,
         {
            physicalTableName,
            storageDestinationName: destinationName,
            sourceEntityId: params.contentSourceEntityId,
         },
      );
      // A CHAINED stored source always rebuilds: an incremental parent's delta
      // can restate rows BELOW this source's frontier, which a half-open range
      // never revisits. Reported under its own reason code.
      const chainedSeed =
         incrementalRefresh !== undefined && dependsOnStorageUpstream;
      if (chainedSeed) {
         reportIncrementalStep({
            step: {
               mode: "seed",
               reasonCode: "chained_storage",
               reason:
                  `the source reads a storage-materialized upstream, and a delta ` +
                  `over a stored parent is not supported yet`,
            },
            sourceName: persistSource.name,
            packageName: incrementalRefresh.context.packageName,
            physicalTableName,
         });
         // Clear the boundary before the rebuild, as the delta path does. A
         // source that becomes chained keeps its name and content address.
         await resetLedger(
            incrementalRefresh.context,
            incrementalRefresh.lineage,
         );
      }
      const refresh: StorageIncrementalRefresh | undefined =
         incrementalRefresh && !chainedSeed
            ? storageRefreshFor({
                 ...incrementalRefresh,
                 persistSource,
                 instruction,
                 destinationName,
                 destinationSchema: destinationSchemaOf(destinationConnection),
                 physicalTableName,
                 buildSQL: params.buildSQL,
                 queryMetadata,
              })
            : undefined;

      // Store each column as the type Malloy declares where one DuckDB type
      // is faithful to it (ms2data/service#6642); checked again below.
      const { declaredColumns, castOptions } = storageBuildCasts(persistSource);

      const startTime = performance.now();
      let result;
      // Set when stacking on the parents was declined and the recompute below
      // ran instead, so the entry can say so and why.
      let upstreamRecomputeReason: string | undefined;

      // Stack on the parent: read its STORED lake table instead of recomputing
      // from raw, so the downstream is a pure function of the parent's rows. On
      // ineligibility it throws and we fall back to recompute (non-strict) or
      // refuse (`strictUpstreams`).
      if (dependsOnStorageUpstream) {
         // An extension table (P10) builds over its parent's stored table from
         // the chain its plan carries, never by lifting author text by name.
         const extension = storedExtensionOf(
            planFacts.materializePlans?.[persistSource.sourceID],
         );
         try {
            // Reads stored parents and writes in the one destination, so its
            // seconds are the store's, as a same-catalog build's are.
            result = await withStatementUsage(
               {
                  environment: environment.getEnvironmentName?.() ?? "",
                  connection: destinationName,
                  surface: "build",
                  package:
                     params.packageName ?? params.incremental?.packageName,
               },
               () =>
                  extension
                     ? this.buildExtensionOverStoredParent(
                          persistSource,
                          extension,
                          destinationName,
                          destinationConnection,
                          builtEntries,
                          environment,
                          physicalTableName,
                          planFacts,
                          ...(params.replacesNameView ? ([true] as const) : []),
                       )
                     : this.seams.buildDownstreamViaParents(
                          persistSource,
                          destinationName,
                          destinationConnection,
                          builtEntries,
                          environment,
                          physicalTableName,
                          planFacts,
                          params.contentSourceEntityId ??
                             instruction.sourceEntityId,
                          lift,
                          // Passed only when set, so a seam's recorded call
                          // is unchanged.
                          ...(params.replacesNameView ? ([true] as const) : []),
                       ),
            );
            recordChainedStorageBuild("parent_reuse");
         } catch (err) {
            // Redact: a failed read-write ATTACH or CTAS can echo the SQL,
            // catalog `password=` included, into the run `error` or the log.
            const safeDetail = redactConnectionSecrets(
               errMessage(err),
               sourceConnection,
               destinationConnection,
            );
            // Only a SHAPE failure justifies recomputing from raw. An INFRA
            // failure (attach, CTAS, destination down) would fail the same way
            // against the same destination, so it throws.
            //
            // Under strict, a missing upstream or an uncarried shape would
            // recompute tables the orchestrator pinned, so it is refused. A
            // downstream that reaches the source warehouse has no build over the
            // parents, so strict permits the recompute, and it is reported.
            const pinnedRecompute =
               err instanceof ChainedUpstreamMissingError ||
               err instanceof ChainedShapeNotCarriedError;
            if (
               !pinnedRecompute &&
               !(err instanceof MaterializationEligibilityError)
            ) {
               recordChainedStorageBuild("infra_failure");
               throw new Error(
                  `Failed to materialize chained source '${persistSource.name}' ` +
                     `into storage destination '${destinationName}': ${safeDetail}`,
               );
            }
            // A host-instructed build refuses a host-supplied upstream entry
            // at the wrong tier, strict or not, instead of recomputing it
            // (#7087, decided 2026-11-12).
            // Only the upstreams whose own entry is at the wrong tier are named,
            // each with its own reason: one host-supplied wrong-tier entry is
            // enough to refuse, whatever else is missing.
            const wrongTierUpstreams =
               planFacts.hostInstructed &&
               err instanceof ChainedUpstreamMissingError
                  ? err.details
                     ? err.details.filter((d) =>
                          d.detail.includes(WRONG_TIER_MARKER),
                       )
                     : // A thrower that gives no per-name reasons: the whole
                       // message is the reason for each missing upstream.
                       safeDetail.includes(WRONG_TIER_MARKER)
                       ? err.missing.map((name) => ({
                            name,
                            detail: safeDetail,
                         }))
                       : []
                  : [];
            if (wrongTierUpstreams.length > 0) {
               recordChainedStorageBuild("strict_refused");
               throw new HostSuppliedWrongTierError(
                  persistSource.name,
                  wrongTierUpstreams,
               );
            }
            if (manifest.strict && pinnedRecompute) {
               recordChainedStorageBuild("strict_refused");
               const message =
                  `Failed to materialize chained source '${persistSource.name}' ` +
                  `into storage destination '${destinationName}' by reading ` +
                  `its materialized upstream, and strict upstreams forbid ` +
                  `recomputing it from raw: ${safeDetail}`;
               // Re-raised as the same type, so the run can classify the
               // failure (build_failure_class.ts): a missing upstream that
               // failed earlier in the run, or a shape no retry can carry.
               throw err instanceof ChainedUpstreamMissingError
                  ? new ChainedUpstreamMissingError(err.missing, message)
                  : new ChainedShapeNotCarriedError(message);
            }
            // The orchestrator may refuse the one recompute strict permits
            // (ms2data/service#6756): the source fails with the reason the
            // entry would have carried, and nothing reads the warehouse.
            if (
               manifest.strict &&
               planFacts.warehouseReach === "refuse" &&
               err instanceof MaterializationEligibilityError
            ) {
               recordChainedStorageBuild("strict_refused");
               throw new WarehouseRecomputeRefusedError(
                  persistSource.name,
                  destinationName,
                  safeDetail,
               );
            }
            recordChainedStorageBuild(
               manifest.strict ? "strict_shape_fallback" : "inline_fallback",
            );
            upstreamRecomputeReason = safeDetail;
            logger.warn(
               "Chained storage build could not reuse the parent table; " +
                  "recomputing the upstream from raw",
               {
                  sourceName: persistSource.name,
                  destinationName,
                  strict: manifest.strict,
                  // The reason is on the entry (upstreamRecomputeReason); a
                  // compile error in it can quote the build's text.
                  code: errorCodeOf(err),
               },
            );
            // A recompute because a parent's entry is at the wrong tier reads
            // the warehouse; counted once per run per parent (#7087).
            if (
               err instanceof ChainedUpstreamMissingError &&
               safeDetail.includes(WRONG_TIER_MARKER)
            ) {
               for (const upstream of err.missing) {
                  noteWrongTierRecompute({
                     planFacts,
                     build: "storage",
                     sourceName: persistSource.name,
                     upstream,
                     detail: safeDetail,
                  });
               }
            }
         }
      }

      // Recompute from raw: run `buildSQL` (storage upstreams inlined) in the
      // source warehouse and CTAS it into the destination.
      if (!result) {
         try {
            const build = () =>
               buildSourceIntoStorage({
                  destinationName,
                  destinationConnection,
                  sourceConnection,
                  buildSQL: params.publicBuildSQL,
                  sourceCarriesAuthorSql: carriesAuthorSql(
                     persistSource._sourceDef,
                  ),
                  physicalTableName,
                  partitionColumns: partitionColumnsForBuild(persistSource),
                  declaredColumns,
                  castOptions,
                  environmentPath: environment.getEnvironmentPath(),
                  queryMetadata,
                  incremental: refresh,
                  ...(params.replacesNameView
                     ? { replacesNameView: true }
                     : {}),
               });
            // A same-catalog build reads and writes only the store, so its seconds
            // are the store's; a passthrough build's are mostly the warehouse's.
            result = isSameCatalog(sourceConnection, destinationConnection)
               ? await withStatementUsage(
                    {
                       environment: environment.getEnvironmentName?.() ?? "",
                       connection: destinationName,
                       surface: "build",
                       package:
                          params.packageName ?? params.incremental?.packageName,
                    },
                    build,
                 )
               : await build();
         } catch (err) {
            // Redact: a federation/passthrough/attach error can echo connection
            // detail (connstrings, service-account JSON). Strip credential VALUES
            // but keep the message legible, since the run `error` is user-visible.
            const safeDetail = redactConnectionSecrets(
               errMessage(err),
               sourceConnection,
               destinationConnection,
            );
            // Whether the warehouse read was already BILLED must survive the
            // rewrap: a scheduled retry re-pays for it every occurrence, unlike a
            // free-to-retry attach failure.
            // The run counts and logs the failure once, by its class
            // (`billed_read_lost` for this one; run_telemetry.ts).
            const alreadyBilled = err instanceof BilledReadNotCapturedError;
            const failure =
               `Failed to materialize source '${persistSource.name}' into ` +
               `storage destination '${destinationName}': ${safeDetail}`;
            throw alreadyBilled
               ? new BilledReadNotCapturedError(failure, { cause: err })
               : new Error(failure, { cause: err });
         }
      }

      // A column no `type:` declares faithfully is left out of its source's
      // declaration at bind, so only a query reading it answers live, and a
      // table with none left is declined there: counted and logged here,
      // never refused.
      reportLeftOutColumns(
         persistSource.name,
         declarableColumns(
            result.schema.flatMap((c) =>
               c.name && c.type ? [{ name: c.name, type: c.type }] : [],
            ),
         ).leftOut,
      );

      // What the cast could not make faithful (a number the warehouse returned
      // as text), or a delta applied to a table an older build typed. Not a
      // refusal: the serve shape leaves such a column off, so only queries
      // that read it are answered live, and the package reports it.
      const mistyped = storedColumnTypeMismatches(
         result.schema.flatMap((c) =>
            c.name && c.type ? [{ name: c.name, type: c.type }] : [],
         ),
         declaredColumns,
      );
      if (mistyped.length > 0) {
         for (const m of mistyped) {
            recordStoredColumnTypeMismatch("build", m.declaredType);
         }
         logger.warn(
            "Storage build stored columns whose type does not serve as the declared Malloy type; queries reading them are answered live",
            {
               code: STORED_COLUMN_TYPE_MISMATCH_CODE,
               sourceName: persistSource.name,
               destinationName,
               physicalTableName,
               columns: mistyped,
            },
         );
      }

      // Make this table visible to downstream sources built later in this run,
      // so a chained storage source can stack on it (above).
      manifest.update(sourceEntityId, { tableName: physicalTableName });

      const durationMs = Math.round(performance.now() - startTime);
      // Debug: the run's `materialization.build.finished` is the build's
      // event, and the build histogram times it (a SKIP is not timed).
      logger.debug(
         result.refresh
            ? `Refreshed materialized source ${persistSource.name} in storage`
            : `Built materialized source ${persistSource.name} into storage`,
         {
            physicalTableName,
            storageDestinationName: result.storageDestinationName,
            columns: result.schema.length,
            durationMs,
            refresh: result.refresh?.refresh,
            // The whole cost and the warehouse job ids, which the manifest has
            // no room for. Null means nothing was reported, not free (see
            // BuildReadCost).
            readCost: result.readCost,
         },
      );

      return {
         sourceEntityId,
         sourceName: persistSource.name,
         materializedTableId: instruction.materializedTableId,
         physicalTableName,
         connectionName: persistSource.connectionName,
         storageDestinationName: result.storageDestinationName,
         schema: result.schema,
         realization: instruction.realization,
         rowCount: result.rowCount,
         // A refresh reports the APPLY's duration, not the session setup. A
         // SKIP reports null rather than skewing the series as a fast build.
         buildDurationMs: result.refresh
            ? (result.refresh.durationMs ?? null)
            : durationMs,
         // Incremental coverage and what this run did, as for a colocated
         // table. Absent for a non-incremental source.
         ...ledgerFields(
            incrementalRefresh?.lineage,
            result.refresh?.coveredThrough ?? result.seededThrough,
         ),
         ...refreshFields(
            result.refresh?.refresh ??
               (incrementalRefresh ? "full" : undefined),
         ),
         // How stored upstreams were read: a chained attempt answers for
         // itself; otherwise the manifest's answer for the passthrough SQL.
         ...(dependsOnStorageUpstream
            ? {
                 upstreamReuse: result.upstreamReuse ?? "recomputed",
                 ...(upstreamRecomputeReason
                    ? { upstreamRecomputeReason }
                    : {}),
              }
            : baseReuse),
         // SCANNED, not billed, matching the colocated path's runStats
         // (totalBytesProcessed), so the field sums consistently across sources.
         // Null when the read reported nothing: a build with an empty metadata
         // bag, since the label is what makes BigQuery's read report.
         queryCostBytes: result.readCost?.bytesScanned ?? null,
      };
   }

   /**
    * Stack on the parent: build a chained storage source over its upstreams'
    * tables in the SAME destination. Rebinds each to a virtual source with its
    * captured schema, re-declares the downstream over them (text lifted from the
    * author's model), and hands the transient model to
    * {@link buildDownstreamIntoStorage}.
    *
    * Throws (the caller then recomputes or refuses) when there is no
    * same-destination upstream, the definition text can't be lifted, or the
    * transient model doesn't compile.
    */
   async buildDownstreamViaParents(
      persistSource: PersistSource,
      destinationName: string,
      destinationConnection: ApiConnection,
      builtEntries: Record<string, ManifestEntry>,
      environment: BuildEnvironment,
      physicalTableName: string,
      planFacts: ChainedPlanFacts,
      /** The content address being built; a walk stop sharing it is this source's own table. */
      rootAddress: string,
      lift: ReturnType<typeof authorModelLiftContext>,
      /** See StorageBuildRequest.replacesNameView. */
      replacesNameView?: boolean,
   ): Promise<StorageBuildResult> {
      // Rebind every upstream in THIS destination under every name for its
      // table: a rename (`source: x is daily`) binds to the same handle, as on
      // the serve path. An EXTENSION adds refinements the table lacks, so it is
      // lifted below over its base instead. A parent in another destination is
      // reported missing below.
      const aliasesForBinding: Record<string, string[]> = {};
      for (const [name, aliases] of Object.entries(
         planFacts.aliasesBySourceName,
      )) {
         aliasesForBinding[name] = aliases.filter(
            (alias) => typeof lift.contents[alias]?.extends !== "string",
         );
      }
      // Only an entry at the destination its source is declared in NOW: a
      // stored table left from before the source moved is stale (#7087).
      const currentEntries = currentUpstreamEntries(builtEntries, planFacts);
      const upstreams: ServeBinding[] = deriveServeBindings(
         currentEntries,
         aliasesForBinding,
         (name) => {
            try {
               return destinationSchemaOf(
                  environment.getStorageDestination(name),
               );
            } catch {
               return undefined;
            }
         },
         planFacts.sourceIDsFor,
      )
         .filter((b) => b.destinationName === destinationName)
         // A binding names the source by NAME, and the lift below skips any
         // candidate whose name is bound. So an upstream materializing another
         // model's same-named source would stand in for this model's own and be
         // built into the table (malloydata/publisher#1001). Kept only where the
         // downstream's model means that source.
         .filter((b) => bindingServesModelSource(b, lift.contents))
         // A rollup is never an upstream (its synthesized name is in no model
         // file). Filtering it also keeps the legible "no materialized upstream"
         // refusal for a destination holding only rollups.
         .filter((b) => b.origin !== "preaggregate");
      const present = new Set(upstreams.map((b) => b.sourceName));
      // Stored tables the downstream reads, directly or transitively. One absent
      // from this destination's parents is named here, before any compile, which
      // would report only "undefined object".
      const reached = reachPersisted(
         lift,
         persistSource.name,
         (_name, sourceID) => planFacts.persistSourceIds.has(sourceID),
      );
      // The stops that are upstreams: one sharing the root's address is the
      // table being built (a persisted extension and its base), not a parent.
      const stops = reached.persisted.filter(
         (p) => planFacts.addressBySourceId[p.sourceID] !== rootAddress,
      );
      const missing = missingPersistedTables(
         stops.map((p) => p.name),
         planFacts.aliasesBySourceName,
         present,
      );
      if (missing.length > 0) {
         // Say where the table is, not only where it is not.
         const where = (name: string): string => {
            const sourceID = reached.persisted.find(
               (p) => p.name === name,
            )?.sourceID;
            const address =
               sourceID === undefined
                  ? undefined
                  : planFacts.addressBySourceId[sourceID];
            return upstreamPlacement(
               name,
               address,
               address === undefined ? undefined : builtEntries[address],
               destinationName,
               planFacts,
            );
         };
         const details = missing.map((name) => ({ name, detail: where(name) }));
         throw new ChainedUpstreamMissingError(
            missing,
            `persisted upstream of '${persistSource.name}': ` +
               `${details.map((d) => d.detail).join("; ")} — the build cannot ` +
               `stack on it, and the recompute that remains would rebuild the ` +
               `stored tables its SQL inlines`,
            details,
         );
      }
      // A path to the source warehouse (a joined table, a SQL source) has no
      // stored stand-in, so only a recompute can build it. Decided from the
      // model, not the compile, because strict treats "cannot express" and
      // "could not carry" differently.
      if (reached.raw) {
         throw new MaterializationEligibilityError({
            message:
               `'${persistSource.name}' reads the source warehouse ` +
               `(${reached.rawLeaves.join(", ")}) through ` +
               `${reached.rawVia.map((n) => `'${n}'`).join(", ")}, which no ` +
               `stored table can stand in for`,
         });
      }
      if (upstreams.length === 0) {
         throw new ChainedShapeNotCarriedError(
            "no materialized upstream is available in this destination to build on",
         );
      }
      const notCarried = (detail: string) =>
         new ChainedShapeNotCarriedError(
            `'${persistSource.name}' reaches only stored upstreams ` +
               `(${stops.map((p) => `'${p.name}'`).join(", ")}), but ` +
               `the build could not express it over them — a refinement ` +
               `declared on a stored upstream that the build does not ` +
               `re-declare, or a construct the destination's dialect lacks: ` +
               detail,
         );
      // The served copy writes the text: each upstream re-rooted on its table
      // here as a query would read it, from this run's entries alone.
      if (!environment.chainBuildText) {
         throw notCarried("this package has no served copy to write it from");
      }
      const byAddress = new Map<string, Record<string, string[] | undefined>>();
      for (const b of upstreams) {
         const names = byAddress.get(b.virtualHandle) ?? {};
         names[b.sourceName] = b.sourceIDs;
         byAddress.set(b.virtualHandle, names);
      }
      const text = environment.chainBuildText({
         downstream: {
            sourceID: persistSource.sourceID,
            name: persistSource.name,
         },
         destination: {
            name: destinationName,
            schema: destinationSchemaOf(destinationConnection),
         },
         upstreams: [...byAddress].map(([address, names]) => ({
            entry: currentEntries[address],
            names,
         })),
      });
      if ("declined" in text) throw notCarried(text.declined);
      try {
         return await buildDownstreamIntoStorage({
            destinationName,
            destinationConnection,
            model: {
               files: text.files,
               entry: text.entry,
               leaveOutWithin: text.rerootedSpans,
            },
            connectionName: text.connection,
            downstreamName: text.downstreamName,
            virtualMap: text.virtualMap,
            physicalTableName,
            partitionColumns: partitionColumnsForBuild(persistSource),
            environmentPath: environment.getEnvironmentPath(),
            ...(replacesNameView ? { replacesNameView } : {}),
         });
      } catch (err) {
         // The model reached only stored parents (checked above), so a shape
         // failure here is one the build could not carry, not one the
         // destination cannot express. Infrastructure failures pass through.
         if (err instanceof MaterializationEligibilityError) {
            throw notCarried(err.message);
         }
         throw err;
      }
   }

   /**
    * Build a `storage=` extension table over its parent's stored table in the
    * same destination (P10): the parent's build source declared as the
    * virtual source of that table, the extension's chain over it
    * ({@link extensionOverStoredModel}), compiled and run on the destination
    * alone, so nothing reads the warehouse. A parent with no current table in
    * this destination is {@link ChainedUpstreamMissingError}, and a chain the
    * destination cannot compile (the plan checked it can) is
    * {@link ChainedShapeNotCarriedError}: the caller recomputes from raw, or,
    * under `strictUpstreams`, refuses, as for any chained build.
    */
   async buildExtensionOverStoredParent(
      persistSource: PersistSource,
      extension: StoredExtension,
      destinationName: string,
      destinationConnection: ApiConnection,
      builtEntries: Record<string, ManifestEntry>,
      environment: BuildEnvironment,
      physicalTableName: string,
      planFacts: ChainedPlanFacts,
      /** See StorageBuildRequest.replacesNameView. */
      replacesNameView?: boolean,
   ): Promise<StorageBuildResult> {
      const { plan, parent, chain } = extension;
      const address = planFacts.addressBySourceId[parent.sourceID];
      const entry = address === undefined ? undefined : builtEntries[address];
      if (
         address === undefined ||
         entry === undefined ||
         !upstreamEntryIsCurrent(address, entry, planFacts) ||
         entry.storageDestinationName !== destinationName ||
         !entry.schema?.length
      ) {
         const detail = upstreamPlacement(
            parent.sourceName,
            address,
            entry,
            destinationName,
            planFacts,
         );
         throw new ChainedUpstreamMissingError(
            [parent.sourceName],
            `persisted upstream of '${persistSource.name}': ${detail} — the ` +
               `build cannot stack on it, and the recompute that remains ` +
               `would rebuild the stored table its SQL inlines`,
            [{ name: parent.sourceName, detail }],
         );
      }
      const binding: ServeBinding = {
         sourceName: chain.parentBuildSourceName,
         destinationName,
         virtualHandle: address,
         tablePath: storageTablePath(
            destinationName,
            entry.physicalTableName,
            destinationSchemaOf(destinationConnection),
         ),
         schema: entry.schema
            .filter((c) => c.name && c.type)
            .map((c) => ({ name: c.name as string, type: c.type as string })),
      };
      // A parent column no `type:` declares faithfully is left out: the
      // extension's build selects its columns by name, so one reading it does
      // not compile and is recomputed, as for any chain it cannot carry.
      const { declared, leftOut } = declarableColumns(binding.schema);
      reportLeftOutColumns(parent.sourceName, leftOut);
      try {
         return await buildDownstreamIntoStorage({
            destinationName,
            destinationConnection,
            model: oneFileModel(
               extensionOverStoredModel({
                  plan,
                  chain,
                  connectionName: destinationName,
                  virtualHandle: address,
                  columns: declared.map((c) => ({
                     name: c.name,
                     type: c.malloyType,
                  })),
               }),
            ),
            downstreamName: plan.buildSourceName,
            virtualMap: buildVirtualMap([binding]),
            physicalTableName,
            partitionColumns: partitionColumnsForBuild(persistSource),
            environmentPath: environment.getEnvironmentPath(),
            ...(replacesNameView ? { replacesNameView } : {}),
         });
      } catch (err) {
         if (!(err instanceof MaterializationEligibilityError)) throw err;
         // Its text names the generated sources; logged, never returned.
         logger.warn(
            "A storage= extension table did not build over its parent's stored table",
            { sourceName: persistSource.name, error: errMessage(err) },
         );
         throw new ChainedShapeNotCarriedError(
            `'${persistSource.name}' could not be built over the stored table ` +
               `of '${parent.sourceName}' in destination '${destinationName}'`,
         );
      }
   }
}

/** A `storage=` extension table (P10) and the parent it builds over. */
interface StoredExtension {
   plan: MaterializePlan;
   parent: MaterializeParent;
   chain: StoredChain;
}

/** `plan` as a {@link StoredExtension}, when its build reads a stored parent. */
function storedExtensionOf(
   plan: MaterializePlan | undefined,
): StoredExtension | undefined {
   const parent = plan?.buildsOver;
   const chain = parent?.overStored;
   return plan && parent && chain ? { plan, parent, chain } : undefined;
}

/**
 * Where a persisted upstream's table is, for a chained build that cannot
 * stack on it in `destinationName`: at the wrong tier, without a column list,
 * in another destination, outside any destination, or nowhere this build has.
 */
function upstreamPlacement(
   name: string,
   address: string | undefined,
   entry: ManifestEntry | undefined,
   destinationName: string,
   planFacts: ChainedPlanFacts,
): string {
   if (
      entry !== undefined &&
      address !== undefined &&
      !upstreamEntryIsCurrent(address, entry, planFacts)
   ) {
      return wrongTierDetail(name, address, entry, planFacts);
   }
   if (
      entry?.storageDestinationName === destinationName &&
      !entry.schema?.length
   ) {
      return (
         `'${name}' is materialized in destination '${destinationName}', ` +
         `but by a reference that carries no column list, which this ` +
         `build needs to read it`
      );
   }
   if (entry?.storageDestinationName !== undefined) {
      return (
         `'${name}' is materialized in destination ` +
         `'${entry.storageDestinationName}', not '${destinationName}'`
      );
   }
   if (entry !== undefined) {
      return (
         `'${name}' is materialized outside destination ` +
         `'${destinationName}' (a warehouse table, or a reference ` +
         `that does not say where it lives), which this build cannot read`
      );
   }
   return (
      `'${name}' is not materialized in destination ` +
      `'${destinationName}' for this build`
   );
}
