// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   Connection as MalloyConnection,
   Manifest,
   PersistSource,
} from "@malloydata/malloy";
import { ChainedUpstreamMissingError } from "../../../errors";
import { logger } from "../../../logger";
import {
   recordChainedStorageBuild,
   recordStorageBuildFailure,
} from "../../../materialization_metrics";
import { getPersistStorageMode } from "../../../config";
import { mergeQueryMetadata, type QueryMetadata } from "../../query_metadata";
import {
   buildSQLOf,
   projectToPublicColumns,
   resolveQueryMetadata,
} from "../plan/build_plan";
import {
   authorModelLiftContext,
   reachedPersistedSources,
} from "../serve/materialization_serve_transform";
import { BuildInstruction, ManifestEntry } from "../types";
import type { BuildStrategy, ColocatedBuildRequest } from "./build_strategy";
import { ColocatedCtasStrategy } from "./colocated_build_strategy";
import {
   sourceIncrementalRefresh,
   type IncrementalRunContext,
} from "./incremental_build";
import { IncrementalDeltaStrategy } from "./incremental_delta_strategy";
import {
   BuildEnvironment,
   BuildQueryMetadata,
   ChainedPlanFacts,
   compilerMissingUpstream,
   connectionMetadataLayers,
   manifestExcludingStorage,
   manifestForCompiler,
   NO_PLAN_FACTS,
   readAuthorFile,
   upstreamReuseFromManifest,
} from "./materialization_run_support";
import type { StorageBuildRequest } from "./storage_build_strategy";

/** The colocated tier, in the order it is tried: a delta, else a full rebuild. */
const COLOCATED_STRATEGIES: readonly BuildStrategy<ColocatedBuildRequest>[] = [
   new IncrementalDeltaStrategy(),
   new ColocatedCtasStrategy(),
];

/**
 * One source's build, whatever its tier: render its SQL over the run's
 * manifest, decide once how its stored upstreams are read, resolve the query
 * metadata every statement carries, then hand it to the storage tier or to the
 * colocated strategies (see build_strategy.ts).
 */
export class SourceBuilder {
   constructor(
      /** The storage tier (StorageBuildStrategy, through the facade's seam). */
      private readonly storage: (
         request: StorageBuildRequest,
      ) => Promise<ManifestEntry>,
   ) {}

   /**
    * Build a single instructed source into its assigned physical table.
    * COPY uses a staging table + atomic rename for crash-safety; the staging
    * name derives from the sourceEntityId. Records and returns the manifest entry.
    */
   async build(
      persistSource: PersistSource,
      instruction: BuildInstruction,
      connection: MalloyConnection,
      manifest: Manifest,
      environment: BuildEnvironment,
      builtEntries: Record<string, ManifestEntry>,
      buildMetadata?: BuildQueryMetadata,
      // Absent leaves the full-rebuild path.
      incremental?: IncrementalRunContext,
      // The content address, NOT the caller-assigned
      // `instruction.sourceEntityId`. Recorded on the covered_through ledger and
      // COMPARED on every read, so a boundary is never read against different
      // SQL: a changed source seeds instead of deltaing onto a stale table. It is
      // not part of the ledger key, so a boundary stays findable across versions.
      contentSourceEntityId?: string,
      // The default knows nothing; only test seams rely on it.
      planFacts: ChainedPlanFacts = NO_PLAN_FACTS,
   ): Promise<ManifestEntry> {
      const physicalTableName = instruction.physicalTableName;
      const isStorageBuild =
         !!instruction.destination && getPersistStorageMode() !== "off";
      // Warehouse-executed build SQL cannot see a storage upstream's
      // DuckDB/DuckLake table, so storage upstreams are excluded from the build
      // manifest: non-strict they INLINE (recompute from raw); under
      // `strictUpstreams` they are a strict-miss error.
      //
      // A storage build renders this SQL permissively because for it the SQL is
      // only the recompute fallback (stacking on the parent uses a separate
      // DuckDB recompile); rendering strictly would refuse a chained source
      // before parent reuse could run. Strict is enforced below, where a
      // recompute would really happen. A colocated build keeps strict: its
      // buildSQL IS what runs.
      const reducedManifest = manifestExcludingStorage(manifest, builtEntries);
      const buildManifest = isStorageBuild
         ? { ...reducedManifest, strict: false }
         : reducedManifest;
      const buildSQL = buildSQLOf(persistSource, buildManifest);

      // Which stored tables this source reads, and whether the substituted
      // manifest holds each, decided once for both tiers. It drives stacking on
      // parents, strict refusal, and the entry's `upstreamReuse`.
      //
      // `_model._modelDef` is compiler-internal. If a compiler bump removes it,
      // the walk finds nothing and the entry carries no `upstreamReuse`: a
      // visible gap, not a wrong answer.
      const lift = authorModelLiftContext(
         persistSource._model?._modelDef,
         readAuthorFile,
      );
      const reached = reachedPersistedSources(
         lift,
         persistSource.name,
         (_name, sourceID) => planFacts.persistSourceIds.has(sourceID),
      );
      // The compiler's reach, rendered twice over the full manifest.
      // Permissively, compared with the build SQL: a difference means a stored
      // table was inlined. Strictly: the one possible miss is a persist source
      // the SQL reads that nothing supplied.
      //
      // These renders only classify. A colocated build logs a failed render and
      // omits `upstreamReuse`; a storage build fails, because stacking on parents
      // needs the answer. Neither runs when nothing was excluded, and the strict
      // render is skipped for a strict colocated build (already strict above).
      const fullForCompiler = manifestForCompiler(
         manifest.buildManifest,
         planFacts,
      );
      const excludedSomething =
         Object.keys(reducedManifest.entries).length !==
         Object.keys(manifest.buildManifest.entries).length;
      let sqlInlinesStored = false;
      let compilerMissing: { name: string; sourceID: string }[] = [];
      let classified = true;
      try {
         if (excludedSomething) {
            sqlInlinesStored =
               buildSQLOf(persistSource, {
                  ...fullForCompiler,
                  strict: false,
               }) !== buildSQL;
         }
         if (isStorageBuild || !manifest.strict) {
            compilerMissing = compilerMissingUpstream(
               persistSource,
               fullForCompiler,
               lift.sourceNameById,
            );
         }
      } catch (err) {
         if (isStorageBuild) throw err;
         classified = false;
         logger.warn(
            "Could not classify a colocated build's stored upstreams; the entry carries no upstreamReuse",
            {
               sourceName: persistSource.name,
               error: err instanceof Error ? err.message : String(err),
            },
         );
      }
      const reuse = classified
         ? upstreamReuseFromManifest({
              reached: reached.persisted,
              addressBySourceId: planFacts.addressBySourceId,
              substituted: buildManifest.entries,
              full: manifest.buildManifest.entries,
              builtEntries,
              tier: isStorageBuild ? "storage" : "colocated",
              sqlInlinesStored,
              compilerMissing,
              // The content address: orchestrators may assign their own ids.
              rootAddress: contentSourceEntityId ?? instruction.sourceEntityId,
           })
         : { fields: {}, inManifestOnly: [], missing: [] };

      // Every statement of this build carries the same metadata so query history
      // shows it as one unit of work. Resolved before the storage branch so both
      // paths attribute identically (a `storage=` build applies it itself).
      const runOptions = this.buildRunSQLOptions(
         persistSource,
         environment,
         buildMetadata,
      );

      // `storage=` build into a DuckDB/DuckLake destination via a build-scoped
      // session (never the source or serve connection), capturing the
      // authoritative schema for the serve transform.
      if (isStorageBuild) {
         // An upstream nothing supplied can only be recomputed, which strict
         // forbids. The build SQL was rendered permissively, so the refusal is
         // made here from the strict render (`compilerMissing`).
         if (manifest.strict && reuse.missing.length > 0) {
            recordChainedStorageBuild("strict_refused");
            recordStorageBuildFailure(instruction.destination!);
            // Typed, so the failure can be attributed to an upstream that
            // failed earlier in this run (see build_failure_class.ts).
            throw new ChainedUpstreamMissingError(
               reuse.missing,
               `Failed to materialize source '${persistSource.name}' into ` +
                  `storage destination '${instruction.destination}': persisted ` +
                  `upstream ${reuse.missing.map((n) => `'${n}'`).join(", ")} is ` +
                  `not in this build's manifest (neither built in this run nor ` +
                  `supplied by reference), and strict upstreams forbid ` +
                  `recomputing it from raw`,
            );
         }
         // Stack on the parent exactly when the build SQL inlined a stored
         // table (the two renders differ). The walk alone must not decide: a
         // declared join the query never reads needs no chained attempt.
         const dependsOnStorageUpstream = sqlInlinesStored;
         // Materialize ONLY the source's PUBLIC columns. The build SQL projects
         // hidden ones too (`except:`, access modifiers); serving is already
         // bounded by the shape, so this keeps hidden VALUES from sitting at
         // rest in the destination, reachable by direct catalog access. Refuses
         // the build (422) if the public surface can't be determined.
         const publicBuildSQL = projectToPublicColumns(persistSource, buildSQL);
         return this.storage({
            persistSource,
            instruction,
            manifest,
            environment,
            publicBuildSQL,
            // Unprojected, for a delta: it projects outermost, around its range
            // predicate, since the projection may not carry the watermark.
            buildSQL,
            builtEntries,
            dependsOnStorageUpstream,
            // Reported when the build does not stack on a parent.
            baseReuse: reuse.fields,
            queryMetadata: runOptions.queryMetadata,
            packageName: buildMetadata?.context.package,
            incremental,
            contentSourceEntityId,
            planFacts,
            lift,
         });
      }

      // Incremental refresh: a source with `refresh="incremental"` and a usable
      // watermark advances by a bounded delta; anything else gets `undefined`
      // and falls through to the CTAS. One narrowing, so the incremental
      // touchpoints never disagree.
      const request: ColocatedBuildRequest = {
         persistSource,
         instruction,
         connection,
         manifest,
         buildSQL,
         runOptions,
         upstreamReuse: reuse.fields,
         incrementalRefresh: sourceIncrementalRefresh(
            incremental,
            persistSource,
            { physicalTableName, sourceEntityId: contentSourceEntityId },
         ),
      };
      for (const strategy of COLOCATED_STRATEGIES) {
         const entry = await strategy.build(request);
         if (entry) return entry;
      }
      throw new Error(
         `No colocated build strategy built source '${persistSource.name}'`,
      );
   }

   /**
    * The `RunSQLOptions` for one source's build statements. Layers, least
    * specific first: connection default, the model's declaration (via
    * {@link resolveQueryMetadata}), then the run's context naming the source.
    *
    * Fails OPEN: a build must not fail over metadata, so an unresolvable
    * connection contributes no default and a dropped property is logged and
    * metered rather than thrown.
    */
   private buildRunSQLOptions(
      persistSource: PersistSource,
      environment: BuildEnvironment,
      buildMetadata: BuildQueryMetadata | undefined,
   ): { queryMetadata?: QueryMetadata } {
      if (!buildMetadata) return {};
      const connectionLayers = connectionMetadataLayers(
         environment,
         persistSource.connectionName,
      );
      const resolved = mergeQueryMetadata({
         connection: connectionLayers.default,
         enforced: connectionLayers.enforced,
         model: resolveQueryMetadata(
            persistSource,
            buildMetadata.packageMaterialization,
         ),
         context: { ...buildMetadata.context, source: persistSource.name },
      });
      if (resolved.drops.length > 0) {
         logger.warn("Dropped query-metadata properties for a build", {
            sourceName: persistSource.name,
            drops: resolved.drops,
         });
      }
      return resolved.metadata ? { queryMetadata: resolved.metadata } : {};
   }
}
