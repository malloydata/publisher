// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One source's build, whatever its tier ({@link SourceBuilder}): render its
 * build SQL over the run's manifest, decide once how its stored upstreams are
 * read, resolve the query metadata every statement carries, then hand it to
 * the colocated or `storage=` tier's adapter (tier.ts). Called by the
 * instructed build, and replaceable as one of the service's run collaborators.
 *
 * Refuses an upstream a host supplied at the wrong tier and names a strict
 * miss by the author's source. Counts each chained storage build.
 */

import { plannedModelDefOf } from "../../governance/ir/compiled_model";
import type {
   Connection as MalloyConnection,
   Manifest,
   PersistSource,
} from "@malloydata/malloy";
import { ChainedUpstreamMissingError } from "../../../errors";
import { logger } from "../../../logger";
import { recordChainedStorageBuild } from "../../../materialization_metrics";
import { getPersistStorageMode } from "../../../config";
import { mergeQueryMetadata, type QueryMetadata } from "../../query_metadata";
import {
   buildSQLOf,
   projectToStoredColumns,
   resolveQueryMetadata,
} from "../plan/build_plan";
import { authorModelLiftContext } from "../text/author_model";
import { reachPersisted } from "../text/source_map";
import { BuildInstruction, ManifestEntry } from "../domain/types";
import type { ColocatedBuildRequest } from "./tier";
import { ColocatedTier } from "./colocated_build_strategy";
import {
   sourceIncrementalRefresh,
   type IncrementalRunContext,
} from "./incremental_build";
import {
   BuildEnvironment,
   BuildQueryMetadata,
   ChainedPlanFacts,
   compilerMissingUpstream,
   connectionMetadataLayers,
   manifestExcludingStorage,
   manifestForCompiler,
   NO_PLAN_FACTS,
   noteWrongTierRecompute,
   presentedSourceName,
   strictMissSourceId,
   HostSuppliedWrongTierError,
   upstreamEntryIsCurrent,
   upstreamReuseFromManifest,
   wrongTierDetail,
} from "./materialization_run_support";
import type {
   StorageBuildRequest,
   StoredRollupBase,
} from "./storage_build_strategy";
import { compiledTextReader } from "../../../utils";

/**
 * Render a source's build SQL over the run's manifest, refusing an upstream a
 * host supplied at the wrong tier and naming a strict miss by the author's
 * source; with what the later stages read from the render.
 */
function renderBuildSQL(
   persistSource: PersistSource,
   instruction: BuildInstruction,
   manifest: Manifest,
   environment: BuildEnvironment,
   builtEntries: Record<string, ManifestEntry>,
   planFacts: ChainedPlanFacts,
) {
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
   const reducedManifest = manifestExcludingStorage(
      manifest,
      builtEntries,
      planFacts,
   );
   const buildManifest = isStorageBuild
      ? { ...reducedManifest, strict: false }
      : reducedManifest;
   // Entries left out of the warehouse render only because they are at the
   // wrong tier for the upstream as declared now, by address.
   const wrongTier = Object.entries(builtEntries).filter(
      ([address, entry]) =>
         !entry.storageDestinationName &&
         address in manifest.buildManifest.entries &&
         !upstreamEntryIsCurrent(address, entry, planFacts),
   );
   // The wrong-tier upstreams a strict render misses, by the name its
   // error quotes; a reference carries no sourceName, so the plan's names
   // for the address are tried too.
   const staleMisses = (err: unknown) => {
      const message = err instanceof Error ? err.message : String(err);
      return wrongTier.flatMap(([address, entry]) => {
         const name = [
            ...(entry.sourceName ? [entry.sourceName] : []),
            ...(planFacts.namesByAddress?.[address] ?? []),
         ].find((n) => message.includes(`'${n}'`));
         return name ? [{ address, entry, name }] : [];
      });
   };
   const refuseHostSupplied = (
      stale: ReturnType<typeof staleMisses>,
   ): never => {
      throw new HostSuppliedWrongTierError(
         persistSource.name,
         stale.map((s) => ({
            name: s.name,
            detail: wrongTierDetail(s.name, s.address, s.entry, planFacts),
         })),
      );
   };
   // A host-instructed colocated build refuses, strict or not, an upstream
   // the host supplied at the wrong tier, when this source reads it. A
   // source reads an upstream exactly when adding that upstream's entry back
   // changes its permissive render (the table replaces the inlined SQL);
   // permissive renders never throw, so a genuinely missing upstream cannot
   // hide one, and the check does not depend on which miss a strict render
   // happens to report first. The wrong-tier entry is rendered only to
   // compare, never run.
   if (!isStorageBuild && planFacts.hostInstructed && wrongTier.length > 0) {
      const render = (entries: typeof buildManifest.entries) =>
         buildSQLOf(
            persistSource,
            manifestForCompiler(
               { ...buildManifest, entries, strict: false },
               planFacts,
            ),
         );
      let base: string | undefined;
      try {
         base = render(buildManifest.entries);
      } catch {
         // The source does not render at all; the build below reports why.
      }
      if (base !== undefined) {
         const read = wrongTier.flatMap(([address, entry]) => {
            let withEntry: string;
            try {
               withEntry = render({
                  ...buildManifest.entries,
                  [address]: manifest.buildManifest.entries[address],
               });
            } catch {
               return [];
            }
            if (withEntry === base) return [];
            const name =
               entry.sourceName ??
               planFacts.namesByAddress?.[address]?.[0] ??
               address;
            return [{ address, entry, name }];
         });
         if (read.length > 0) refuseHostSupplied(read);
      }
   }
   // `_model._modelDef` is compiler-internal. If a compiler bump removes it,
   // the walk below finds nothing and the entry carries no
   // `upstreamReuse`: a visible gap, not a wrong answer.
   const plannedDef = plannedModelDefOf(persistSource);
   const lift = authorModelLiftContext(
      plannedDef,
      compiledTextReader(plannedDef, environment.getPackagePath?.()),
   );
   // A strict miss as a reason names it: a name only the generated files
   // declare by the author's source it stands for, read from the
   // companion's own name for it or its declared name.
   const presentedMiss = (sourceID: string, name?: string) => {
      for (const n of [name, sourceID.split("@")[0]]) {
         if (n === undefined) continue;
         const presented = presentedSourceName(n, planFacts);
         if (presented !== n) return presented;
      }
      return undefined;
   };
   let buildSQL: string;
   try {
      buildSQL = buildSQLOf(persistSource, buildManifest);
   } catch (err) {
      // A strict colocated render misses an upstream whose entry was
      // supplied, at the wrong tier: say that, not Malloy's generic miss.
      const stale = staleMisses(err);
      if (!isStorageBuild && planFacts.hostInstructed && stale.length > 0) {
         refuseHostSupplied(stale);
      }
      if (!isStorageBuild && manifest.strict && stale.length > 0) {
         throw new ChainedUpstreamMissingError(
            stale.map((s) => s.name),
            `Failed to materialize source '${persistSource.name}': ` +
               stale
                  .map((s) =>
                     wrongTierDetail(s.name, s.address, s.entry, planFacts),
                  )
                  .join("; ") +
               `, and strict upstreams forbid recomputing it from raw`,
         );
      }
      // A strict miss of a `#(materialize)` parent names its generated
      // build source, in a file the package never shows: refused by the
      // author's name instead, so it is attributed to that source.
      const missed = strictMissSourceId(err)?.sourceID;
      const parent =
         missed === undefined
            ? undefined
            : presentedMiss(missed, lift.sourceNameById.get(missed));
      if (parent !== undefined) {
         throw new ChainedUpstreamMissingError(
            [parent],
            `Failed to materialize source '${persistSource.name}': ` +
               `persisted upstream '${parent}' is not in this build's ` +
               `manifest (neither built in this run nor supplied by ` +
               `reference), and strict upstreams forbid recomputing it ` +
               `from raw`,
         );
      }
      throw err;
   }
   return {
      isStorageBuild,
      reducedManifest,
      buildManifest,
      wrongTier,
      lift,
      presentedMiss,
      buildSQL,
   };
}

/**
 * Classify the stored upstreams a source reads, once for both tiers: the
 * entry's `upstreamReuse` fields, what the substituted manifest is missing,
 * and whether the build SQL inlined a stored table. A colocated build that
 * recomputes a wrong-tier upstream notes it; a host-instructed one refuses.
 */
function classifyStoredUpstreams(
   persistSource: PersistSource,
   instruction: BuildInstruction,
   manifest: Manifest,
   builtEntries: Record<string, ManifestEntry>,
   contentSourceEntityId: string | undefined,
   planFacts: ChainedPlanFacts,
   rendered: ReturnType<typeof renderBuildSQL>,
) {
   const {
      isStorageBuild,
      reducedManifest,
      buildManifest,
      wrongTier,
      lift,
      presentedMiss,
      buildSQL,
   } = rendered;
   // Which stored tables this source reads, and whether the substituted
   // manifest holds each, decided once for both tiers. It drives stacking on
   // parents, strict refusal, and the entry's `upstreamReuse`.
   const reached = reachPersisted(lift, persistSource.name, (_name, sourceID) =>
      planFacts.persistSourceIds.has(sourceID),
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
         ).map((miss) => {
            const presented = presentedMiss(miss.sourceID, miss.name);
            return presented ? { ...miss, name: presented } : miss;
         });
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
   // A colocated build that recomputes an upstream because its entry is at
   // the wrong tier reads the warehouse for it: logged and counted once per
   // run per upstream, since the entry fields alone hide the cost.
   // A host-instructed build refuses instead (HostSuppliedWrongTierError):
   // the host handed it another tier's table, which it must not.
   if (!isStorageBuild && wrongTier.length > 0) {
      const wrongTierAddresses = new Map(wrongTier);
      const refused: { name: string; detail: string }[] = [];
      for (const { name, sourceID } of reached.persisted) {
         const address = planFacts.addressBySourceId[sourceID];
         const entry =
            address === undefined ? undefined : wrongTierAddresses.get(address);
         if (!entry || !reuse.inManifestOnly.includes(name)) continue;
         const detail = wrongTierDetail(name, address!, entry, planFacts);
         if (planFacts.hostInstructed) {
            refused.push({ name, detail });
            continue;
         }
         noteWrongTierRecompute({
            planFacts,
            build: "colocated",
            sourceName: persistSource.name,
            upstream: name,
            detail,
         });
      }
      if (refused.length > 0) {
         throw new HostSuppliedWrongTierError(persistSource.name, refused);
      }
   }
   return { reuse, sqlInlinesStored, lift };
}

/** The colocated tier's adapter: a delta, else a full rebuild. */
const COLOCATED = new ColocatedTier();

/** One source's build: what {@link buildSource} reads. */
export interface SourceBuildOptions {
   persistSource: PersistSource;
   instruction: BuildInstruction;
   connection: MalloyConnection;
   manifest: Manifest;
   environment: BuildEnvironment;
   builtEntries: Record<string, ManifestEntry>;
   buildMetadata?: BuildQueryMetadata;
   /** Absent leaves the full-rebuild path. */
   incremental?: IncrementalRunContext;
   /**
    * The content address, NOT the caller-assigned
    * `instruction.sourceEntityId`. Recorded on the covered_through ledger and
    * COMPARED on every read, so a boundary is never read against different
    * SQL: a changed source seeds instead of deltaing onto a stale table. It
    * is not part of the ledger key, so a boundary stays findable across
    * versions.
    */
   contentSourceEntityId?: string;
   /** The default knows nothing; only test seams rely on it. */
   planFacts?: ChainedPlanFacts;
   /**
    * Statements that drop this package's own leftover `name=` view at the
    * table's name, run first in the build's own transaction (DuckDB,
    * Postgres; leftover_name_views.ts).
    */
   replacesView?: string[];
   /**
    * A `storage=` rollup's base stored in its destination, which the run
    * holds current and fresh enough (instructed_build.ts): the build groups
    * that table's rows in the destination, and reads the warehouse only
    * when it cannot.
    */
   storedBase?: StoredRollupBase;
   /**
    * The source is a rollup: a `storage=` build reports `upstreamReuse`
    * only when it stacked on a stored table, never from the manifest's
    * classification of its author's text (which names the base).
    */
   rollup?: boolean;
}

/**
 * One source's build, whatever its tier: render its SQL over the run's
 * manifest, decide once how its stored upstreams are read, resolve the query
 * metadata every statement carries, then hand it to its tier's adapter (see
 * tier.ts): the colocated tier here, the `storage=` tier through `storage`
 * (StorageBuildStrategy, through the service's collaborators). Records and
 * returns the manifest entry. A colocated COPY uses a staging table and an
 * atomic rename for crash-safety; the staging name derives from the
 * sourceEntityId.
 */
export async function buildSource(
   options: SourceBuildOptions,
   storage: (request: StorageBuildRequest) => Promise<ManifestEntry>,
): Promise<ManifestEntry> {
   const {
      persistSource,
      instruction,
      connection,
      manifest,
      environment,
      builtEntries,
      buildMetadata,
      incremental,
      contentSourceEntityId,
      planFacts = NO_PLAN_FACTS,
      replacesView,
      storedBase,
      rollup,
   } = options;
   const physicalTableName = instruction.physicalTableName;
   const rendered = renderBuildSQL(
      persistSource,
      instruction,
      manifest,
      environment,
      builtEntries,
      planFacts,
   );
   const { isStorageBuild, buildSQL } = rendered;

   const { reuse, sqlInlinesStored, lift } = classifyStoredUpstreams(
      persistSource,
      instruction,
      manifest,
      builtEntries,
      contentSourceEntityId,
      planFacts,
      rendered,
   );

   // Every statement of this build carries the same metadata so query history
   // shows it as one unit of work. Resolved before the storage branch so both
   // paths attribute identically (a `storage=` build applies it itself).
   const runOptions = buildRunSQLOptions(
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
      // Materialize every column the source declares, hidden ones
      // included (`private:`, `internal:`), which the served copy hides
      // again; a column `except:` drops is left out. Refuses the build
      // (422) if the column surface can't be determined.
      return storage({
         persistSource,
         instruction,
         manifest,
         environment,
         storedBuildSQL: projectToStoredColumns(persistSource, buildSQL),
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
         ...(replacesView ? { replacesView } : {}),
         ...(storedBase ? { storedBase } : {}),
         ...(rollup ? { rollup } : {}),
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
      incrementalRefresh: sourceIncrementalRefresh(incremental, persistSource, {
         physicalTableName,
         sourceEntityId: contentSourceEntityId,
      }),
      ...(replacesView ? { replacesView } : {}),
   };
   return COLOCATED.build(request);
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
function buildRunSQLOptions(
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
