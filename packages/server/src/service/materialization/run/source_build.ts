// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One source's build, whatever its tier ({@link SourceBuilder}): render its
 * build SQL over the run's manifest, decide once how its stored upstreams are
 * read, resolve the query metadata every statement carries, then hand it to
 * the database or `storage=` tier's adapter (tier.ts). Called by the
 * instructed build, and replaceable as one of the service's run collaborators.
 *
 * Refuses an upstream a host supplied at the wrong tier and names a strict
 * miss by the author's source. Counts each chained storage build.
 */

import type { StringReads } from "../domain/string_reads";
import type { StoredInDestination } from "../domain/table_plans";
import { errorCodeOf } from "../../../error_code";
import { instructionTier } from "../domain/placement";
import { plannedModelDefOf } from "../../governance/ir/compiled_model";
import type {
   Connection as MalloyConnection,
   Manifest,
   PersistSource,
} from "@malloydata/malloy";
import {
   ChainedUpstreamMissingError,
   SchemaIncompatibleError,
} from "../../../errors";
import { logger } from "../../../logger";
import {
   recordChainedStorageBuild,
   recordUpstreamRecompute,
} from "../../../materialization_metrics";
import { getPersistStorageMode } from "../../../config";
import { mergeQueryMetadata, type QueryMetadata } from "../../query_metadata";
import {
   buildSQLOf,
   projectToStoredColumns,
   resolveQueryMetadata,
} from "./run_plan";
import { authorModelLiftContext } from "../text/author_model";
import { reachPersisted } from "../text/source_map";
import { BuildInstruction, ManifestEntry } from "../domain/types";
import { isPersistedResultSourceID } from "../domain/table_plans";
import type { ExtensionParent } from "./incremental_apply";
import type { DatabaseBuildRequest } from "./tier";
import { DatabaseTier } from "./database_build_strategy";
import {
   sourceIncrementalRefresh,
   type IncrementalRunContext,
} from "./incremental_build";
import {
   BuildEnvironment,
   builtFromOf,
   ChainedPlanFacts,
   compilerMissingUpstream,
   HostSuppliedWrongTierError,
   manifestExcludingStorage,
   manifestForCompiler,
   NO_PLAN_FACTS,
   noteWrongTierRecompute,
   presentedSourceName,
   strictMissSourceId,
   upstreamEntryIsCurrent,
   upstreamReuseFromManifest,
   wrongTierDetail,
} from "./build_upstreams";
import {
   BuildQueryMetadata,
   connectionMetadataLayers,
} from "./build_query_tag";
import type {
   StorageBuildRequest,
   StoredRollupBase,
} from "./storage_build_strategy";
import { compiledTextReader } from "../../../utils";
import { movedSinceBuild, type UpstreamMove } from "./database_column_read";
import { liveCollationsOf } from "./column_contract_capture";

/**
 * The persisted sources `persistSource` reads, from the author's model
 * (`reachPersisted`), with the lift context the walk used: `reached`, the
 * stored tables a build stacks on or reports, and `reachedIds`, those and
 * every persisted source below them, which alone may fill a compiler key
 * they map to ({@link manifestForCompiler}).
 */
export function persistedReach(
   persistSource: PersistSource,
   environment: Pick<BuildEnvironment, "getPackagePath">,
   planFacts: Pick<ChainedPlanFacts, "persistSourceIds">,
): {
   lift: ReturnType<typeof authorModelLiftContext>;
   reached: ReturnType<typeof reachPersisted>;
   reachedIds: ReadonlySet<string>;
} {
   const plannedDef = plannedModelDefOf(persistSource);
   const lift = authorModelLiftContext(
      plannedDef,
      compiledTextReader(plannedDef, environment.getPackagePath?.()),
   );
   // Which stored tables this source reads, and whether the substituted
   // manifest holds each, decided once for both tiers. It drives stacking on
   // parents, strict refusal, and the entry's `upstreamReuse`.
   const isPersisted = (_name: string, sourceID: string) =>
      planFacts.persistSourceIds.has(sourceID);
   const reached = reachPersisted(lift, persistSource.name, isPersisted);
   // An upstream the render inlines (its table absent) is compiled through to
   // what it reads, so the compiler can look up any persisted source below
   // it: those keys belong to this source's reach too.
   const reachedIds = new Set<string>();
   const pending = [...reached.persisted];
   for (let next = pending.pop(); next; next = pending.pop()) {
      if (reachedIds.has(next.sourceID)) continue;
      reachedIds.add(next.sourceID);
      pending.push(...reachPersisted(lift, next.name, isPersisted).persisted);
   }
   return { lift, reached, reachedIds };
}

/**
 * The stored upstreams, by address, whose database table holds other columns
 * than its build recorded ({@link movedSinceBuild}): retyped or dropped in
 * the warehouse since, or never checkable. A build must not read such a
 * table in place of its source, whose address and recorded types say it is
 * built from what that source computes, so the render leaves its entry out
 * and the build recomputes it from its definition, or, under strict
 * upstreams, is refused.
 *
 * Only the database-tier entries the source can read are checked
 * (`reachedIds`), each table once, on the build's own connection, which is
 * the one its SQL reads them through; never one this run built
 * (`builtThisRun`), whose columns were captured moments ago.
 */
async function movedUpstreams(args: {
   reach: ReturnType<typeof persistedReach>;
   manifest: Manifest;
   builtEntries: Record<string, ManifestEntry>;
   planFacts: ChainedPlanFacts;
   connection: MalloyConnection;
   builtThisRun?: ReadonlySet<string>;
   rootAddress?: string;
   sourceName: string;
}): Promise<MovedUpstreams> {
   const { reach, manifest, builtEntries, planFacts } = args;
   const tables = new Map<string, { name: string; entry: ManifestEntry }>();
   for (const sourceID of reach.reachedIds) {
      const address = planFacts.addressBySourceId[sourceID];
      if (address === undefined || address === args.rootAddress) continue;
      if (tables.has(address) || args.builtThisRun?.has(address)) continue;
      const entry = builtEntries[address];
      if (
         !entry ||
         entry.storageDestinationName ||
         !(address in manifest.buildManifest.entries)
      ) {
         continue;
      }
      tables.set(address, {
         name:
            reach.lift.sourceNameById.get(sourceID) ??
            entry.sourceName ??
            address,
         entry,
      });
   }
   if (tables.size === 0) return new Map();
   const reasons = await movedSinceBuild(
      new Map([...tables].map(([address, { entry }]) => [address, entry])),
      args.connection,
   );
   const moved = new Map<string, { name: string } & UpstreamMove>();
   for (const [address, move] of reasons) {
      const { name } = tables.get(address)!;
      if (
         move.kind === "types_unrecorded" &&
         readsUntypedReferences(manifest, planFacts)
      ) {
         noteUntypedReference(args.sourceName, name, "chained");
         continue;
      }
      moved.set(address, { name, ...move });
   }
   return moved;
}

/**
 * Whether a build reads an upstream table whose entry carries no column types
 * unchecked: a strict build a host instructed, whose upstream entries are the
 * host's references. A reference without `nativeSchema` cannot be checked,
 * and strict forbids the recompute, so the table is read as before, with a
 * warning and a count. Any other build recomputes it (movedUpstreams).
 */
export function readsUntypedReferences(
   manifest: Pick<Manifest, "strict">,
   planFacts: Pick<ChainedPlanFacts, "hostInstructed">,
): boolean {
   return manifest.strict === true && planFacts.hostInstructed === true;
}

/** Logs and counts an untyped reference read unchecked ({@link readsUntypedReferences}). */
export function noteUntypedReference(
   sourceName: string,
   upstream: string,
   build: "chained" | "rollup",
): void {
   logger.warn(
      "A strict build reads a referenced upstream table unchecked: the reference carries no nativeSchema, so a column altered since its build cannot be seen; send nativeSchema on each reference",
      { sourceName, upstream },
   );
   recordUpstreamRecompute(
      "reference_types_unrecorded",
      build,
      "read_unchecked",
   );
}

/** {@link movedUpstreams}: each moved upstream's name and why, by address. */
type MovedUpstreams = ReadonlyMap<string, { name: string } & UpstreamMove>;

/** What a build says of a stored upstream whose table moved since its build. */
function movedDetail(name: string, reason: string): string {
   return (
      `persisted upstream '${name}' was not read from its table, since ` +
      `${reason}`
   );
}

/**
 * Render a source's build SQL over the run's manifest, refusing an upstream a
 * host supplied at the wrong tier and naming a strict miss by the author's
 * source; with what the later stages read from the render.
 */
function renderBuildSQL(
   persistSource: PersistSource,
   instruction: BuildInstruction,
   manifest: Manifest,
   builtEntries: Record<string, ManifestEntry>,
   planFacts: ChainedPlanFacts,
   reach: ReturnType<typeof persistedReach>,
   moved: MovedUpstreams,
) {
   const isStorageBuild =
      instructionTier(instruction, getPersistStorageMode()) === "storage";
   // Warehouse-executed build SQL cannot see a storage upstream's
   // DuckDB/DuckLake table, so storage upstreams are excluded from the build
   // manifest: non-strict they INLINE (recompute from raw); under
   // `strictUpstreams` they are a strict-miss error.
   //
   // A storage build renders this SQL permissively because for it the SQL is
   // only the recompute fallback (stacking on the parent uses a separate
   // DuckDB recompile); rendering strictly would refuse a chained source
   // before parent reuse could run. Strict is enforced below, where a
   // recompute would really happen. A database build keeps strict: its
   // buildSQL IS what runs.
   //
   // `reducedManifest` is keyed by address only, as the ledger is; the
   // compiler looks an upstream up by its own key, which differs from the
   // address of a source whose address folds in a `name=`, projection or
   // destination. So the render reads `buildManifest`, which also files each
   // entry under that key: for an upstream this source reads, only that
   // upstream's own entry, never a same-SQL sibling's.
   //
   // `_model._modelDef` is compiler-internal. If a compiler bump removes it,
   // the walk below finds nothing and the entry carries no
   // `upstreamReuse`: a visible gap, not a wrong answer.
   const { lift, reached, reachedIds } = reach;
   // An upstream whose table moved since its build is left out too, so the
   // render recomputes it from its definition (movedUpstreams).
   const reducedManifest = withoutEntries(
      manifestExcludingStorage(manifest, builtEntries, planFacts),
      moved,
   );
   const buildManifest = manifestForCompiler(
      isStorageBuild ? { ...reducedManifest, strict: false } : reducedManifest,
      planFacts,
      reachedIds,
   );
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
   // A host-instructed database build refuses, strict or not, an upstream
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
               reachedIds,
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
   const movedRead = movedUpstreamsRead(
      persistSource,
      manifest,
      reducedManifest,
      planFacts,
      reachedIds,
      moved,
   );
   if (manifest.strict) refuseMovedRead(persistSource.name, movedRead);
   let buildSQL: string;
   try {
      buildSQL = buildSQLOf(persistSource, buildManifest);
   } catch (err) {
      // A strict database render misses an upstream whose entry was
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
      reached,
      reachedIds,
      presentedMiss,
      buildSQL,
      moved,
      movedRead,
   };
}

/**
 * A build that reads an upstream whose table moved since its build is
 * refused, naming each and why ({@link SchemaIncompatibleError}, for the
 * drift handler): it is never recomputed from raw.
 */
function refuseMovedRead(sourceName: string, movedRead: MovedUpstreams): void {
   if (movedRead.size === 0) return;
   const details = [...movedRead.values()].map(({ name, reason }) =>
      movedDetail(name, reason),
   );
   throw new SchemaIncompatibleError(
      `Failed to materialize source '${sourceName}': ` +
         details.join("; ") +
         `, and a downstream is never built by recomputing its upstreams ` +
         `from raw. The package reads its tables and derives its plan again.`,
      [...movedRead.keys()],
   );
}

/** `manifest` without the entries at `addresses`. */
function withoutEntries(
   manifest: Manifest["buildManifest"],
   addresses: ReadonlyMap<string, unknown>,
): Manifest["buildManifest"] {
   if (addresses.size === 0) return manifest;
   return {
      ...manifest,
      entries: Object.fromEntries(
         Object.entries(manifest.entries).filter(([id]) => !addresses.has(id)),
      ),
   };
}

/**
 * The moved upstreams the build actually reads, by address: those whose
 * entry, put back, changes the permissive render (its table would replace
 * the SQL inlined for it). A moved upstream the source does not read (a join
 * the query never uses) changes nothing, and is neither recomputed nor, under
 * strict upstreams, refused.
 */
function movedUpstreamsRead(
   persistSource: PersistSource,
   manifest: Manifest,
   reducedManifest: Manifest["buildManifest"],
   planFacts: ChainedPlanFacts,
   reachedIds: ReadonlySet<string>,
   moved: MovedUpstreams,
): MovedUpstreams {
   if (moved.size === 0) return moved;
   const render = (entries: Manifest["buildManifest"]["entries"]) =>
      buildSQLOf(
         persistSource,
         manifestForCompiler(
            { ...reducedManifest, entries, strict: false },
            planFacts,
            reachedIds,
         ),
      );
   let base: string | undefined;
   try {
      base = render(reducedManifest.entries);
   } catch {
      // Unrenderable: every moved upstream counts as read (fail safe).
   }
   const read = new Map<string, { name: string } & UpstreamMove>();
   for (const [address, upstream] of moved) {
      let withEntry: string | undefined;
      try {
         withEntry =
            base === undefined
               ? undefined
               : render({
                    ...reducedManifest.entries,
                    [address]: manifest.buildManifest.entries[address],
                 });
      } catch {
         // As above.
      }
      if (withEntry === undefined || withEntry !== base) {
         read.set(address, upstream);
      }
   }
   return read;
}

/**
 * Classify the stored upstreams a source reads, once for both tiers: the
 * entry's `upstreamReuse` fields, what the substituted manifest is missing,
 * and whether the build SQL inlined a stored table. A database build that
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
   overStoredUpstreams = false,
) {
   const {
      isStorageBuild,
      reducedManifest,
      buildManifest,
      wrongTier,
      lift,
      reached,
      reachedIds,
      presentedMiss,
      buildSQL,
      moved,
      movedRead,
   } = rendered;
   // The compiler's reach, rendered twice over the full manifest.
   // Permissively, compared with the build SQL: a difference means a stored
   // table was inlined. Strictly: the one possible miss is a persist source
   // the SQL reads that nothing supplied.
   //
   // These renders only classify. A database build logs a failed render and
   // omits `upstreamReuse`; a storage build fails, because stacking on parents
   // needs the answer. Neither runs when nothing was excluded, and the strict
   // render is skipped for a strict database build (already strict above).
   const fullForCompiler = manifestForCompiler(
      manifest.buildManifest,
      planFacts,
      reachedIds,
   );
   // A moved upstream is left out of the render for a reason of its own
   // (`movedRead`), not because a storage destination holds it: compared
   // without it, so `sqlInlinesStored` still means a stored table inlined.
   const fullWithoutMoved = withoutEntries(manifest.buildManifest, moved);
   const excludedSomething =
      Object.keys(reducedManifest.entries).length !==
      Object.keys(fullWithoutMoved.entries).length;
   let sqlInlinesStored = false;
   let compilerMissing: { name: string; sourceID: string }[] = [];
   let classified = true;
   try {
      if (excludedSomething) {
         sqlInlinesStored =
            buildSQLOf(persistSource, {
               ...manifestForCompiler(fullWithoutMoved, planFacts, reachedIds),
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
         "Could not classify a database build's stored upstreams; the entry carries no upstreamReuse",
         {
            sourceName: persistSource.name,
            code: errorCodeOf(err),
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
           tier: isStorageBuild ? "storage" : "database",
           sqlInlinesStored,
           compilerMissing,
           moved: new Map(
              [...moved].map(([address, { name, reason }]) => [
                 address,
                 movedRead.has(address) ? movedDetail(name, reason) : undefined,
              ]),
           ),
           // The content address: orchestrators may assign their own ids.
           rootAddress: contentSourceEntityId ?? instruction.sourceEntityId,
           readsDefinitions:
              !overStoredUpstreams &&
              isPersistedResultSourceID(persistSource.sourceID),
        })
      : { fields: {}, inManifestOnly: [], missing: [], definitionsRead: [] };
   // A persisted view or query reads every upstream through its
   // definition, by design (its companion blanks the source's `#@` lines):
   // labelled `recomputed`, logged and counted, and exempt from strict
   // upstreams, as a database rollup over a stored base is.
   for (const upstream of reuse.definitionsRead) {
      logger.info(
         "A persisted view or query builds from its persisted source's definition, not its table",
         { sourceName: persistSource.name, upstream },
      );
      recordUpstreamRecompute("persisted_result", "chained", "recomputed");
   }
   // A database build that recomputes an upstream because its entry is at
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
            build: "database",
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

/** The database tier's adapter: a delta, else a full rebuild. */
const DATABASE = new DatabaseTier();

/** One source's build: what {@link buildSource} reads. */
export interface SourceBuildOptions {
   /**
    * The run's cancel signal. Passed to the statements that read the
    * warehouse (the CTAS, an incremental delta) and never to a swap, DROP or
    * backup, so a cancel never leaves a half-swapped table.
    */
   signal?: AbortSignal;
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
    * A `storage=` view or query over upstreams in its own destination: the
    * text the plan proved it builds from there (storage_over_stored.ts).
    */
   storedInDestination?: StoredInDestination;
   /** The strings its `storage=` build compares, as its plan recorded them. */
   stringReads?: StringReads;
   /**
    * The source is a rollup: a `storage=` build reports `upstreamReuse`
    * only when it stacked on a stored table, never from the manifest's
    * classification of its author's text (which names the base).
    */
   rollup?: boolean;
   /**
    * A persisted view's or named query's build source compiled over its
    * upstreams' stored tables (persisted_view_compile.ts): it reads each
    * upstream as any chained build does, from its table through the
    * manifest, not through its definition.
    */
   overStoredUpstreams?: boolean;
   /**
    * The addresses this run built so far: their tables' columns were
    * captured by their own builds, so a chained build does not read them
    * again ({@link movedUpstreams}).
    */
   builtThisRun?: ReadonlySet<string>;
}

/**
 * One source's build, whatever its tier: render its SQL over the run's
 * manifest, decide once how its stored upstreams are read, resolve the query
 * metadata every statement carries, then hand it to its tier's adapter (see
 * tier.ts): the database tier here, the `storage=` tier through `storage`
 * (StorageBuildStrategy, through the service's collaborators). Records and
 * returns the manifest entry. A database COPY uses a staging table and an
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
      storedInDestination,
      stringReads,
      rollup,
      signal,
      builtThisRun,
      overStoredUpstreams,
   } = options;
   const physicalTableName = instruction.physicalTableName;
   const reach = persistedReach(persistSource, environment, planFacts);
   const moved = await movedUpstreams({
      reach,
      manifest,
      builtEntries,
      planFacts,
      connection,
      ...(builtThisRun ? { builtThisRun } : {}),
      rootAddress: contentSourceEntityId ?? instruction.sourceEntityId,
      sourceName: persistSource.name,
   });
   const rendered = reportingDrift(environment, () =>
      renderBuildSQL(
         persistSource,
         instruction,
         manifest,
         builtEntries,
         planFacts,
         reach,
         moved,
      ),
   );
   const { isStorageBuild, buildSQL } = rendered;
   const extensionParent = extensionParentOf({
      persistSource,
      planFacts,
      builtEntries,
      builtThisRun,
      incremental,
   });
   // Strict refused these above: here each is recomputed. A rollup's own
   // fallback is counted where it is decided (instructed_build.ts).
   for (const { name, reason, kind } of rendered.movedRead.values()) {
      logger.warn(
         "A stored upstream's table could not be read in place of it; the build recomputes it from its definition",
         { sourceName: persistSource.name, upstream: name, reason, kind },
      );
      if (!rollup) recordUpstreamRecompute(kind, "chained", "recomputed");
   }

   const { reuse, sqlInlinesStored, lift } = classifyStoredUpstreams(
      persistSource,
      instruction,
      manifest,
      builtEntries,
      contentSourceEntityId,
      planFacts,
      rendered,
      // A view or query built in its destination over its upstreams'
      // tables reads them, as one compiled over them does.
      overStoredUpstreams === true || storedInDestination !== undefined,
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
      const built = await storage({
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
         ...(extensionParent ? { extensionParent } : {}),
         ...(replacesView ? { replacesView } : {}),
         ...(storedBase ? { storedBase } : {}),
         ...(storedInDestination ? { storedInDestination } : {}),
         ...(stringReads ? { stringReads } : {}),
         ...(rollup ? { rollup } : {}),
         ...(signal ? { signal } : {}),
         // How its source's strings compare live, for its column contract.
         liveCollations: liveCollationsOf(
            connection,
            buildSQL,
            environment.bigqueryBuildCompareBinary
               ? () =>
                    environment.bigqueryBuildCompareBinary!({
                       connection,
                       source: persistSource,
                       sql: buildSQL,
                    })
               : undefined,
         ),
      });
      return withExtensionReuse(built, planFacts, persistSource.sourceID);
   }

   // Incremental refresh: a source with `refresh="incremental"` and a usable
   // watermark advances by a bounded delta; anything else gets `undefined`
   // and falls through to the CTAS. One narrowing, so the incremental
   // touchpoints never disagree.
   const request: DatabaseBuildRequest = {
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
         ...(extensionParent ? { parent: extensionParent } : {}),
      }),
      ...(replacesView ? { replacesView } : {}),
      ...(signal ? { signal } : {}),
   };
   const built = await DATABASE.build(request);
   // The stored upstream tables its SQL read, as provenance.
   const builtFrom = builtFromOf(
      buildSQL,
      manifest.buildManifest,
      builtEntries,
      contentSourceEntityId ?? instruction.sourceEntityId,
   );
   return withExtensionReuse(
      builtFrom ? { ...built, builtFrom } : built,
      planFacts,
      persistSource.sourceID,
   );
}

/**
 * `entry` labelled `upstreamReuse: "reused"` when it is a `#(materialize)`
 * extension table whose build read its parent's table (its `builtFrom`
 * names the parent's address) and nothing else labelled it. The author's
 * text names the parent, not its generated build source, so the manifest
 * walk that labels other builds does not see it.
 */
function withExtensionReuse(
   entry: ManifestEntry,
   planFacts: ChainedPlanFacts,
   sourceID: string,
): ManifestEntry {
   if (entry.upstreamReuse !== undefined) return entry;
   const parent = planFacts.materializePlans?.[sourceID]?.buildsOver;
   const address = parent && planFacts.addressBySourceId[parent.sourceID];
   if (address === undefined) return entry;
   return (entry.builtFrom ?? []).some((b) => b.sourceEntityId === address)
      ? { ...entry, upstreamReuse: "reused" }
      : entry;
}

/**
 * For an incremental extension table its plan links to its parent's table
 * (`buildsOver.mode.incremental`): the parent as this run holds it, which
 * its refresh follows (incremental_upstream.ts). Undefined for any other
 * source.
 */
function extensionParentOf(args: {
   persistSource: Pick<PersistSource, "sourceID">;
   planFacts: Pick<ChainedPlanFacts, "materializePlans" | "addressBySourceId">;
   builtEntries: Readonly<Record<string, ManifestEntry>>;
   builtThisRun: ReadonlySet<string> | undefined;
   incremental: IncrementalRunContext | undefined;
}): ExtensionParent | undefined {
   const over =
      args.planFacts.materializePlans?.[args.persistSource.sourceID]
         ?.buildsOver;
   const coordinate = over?.mode.incremental?.coordinate;
   if (!over || !coordinate) return undefined;
   const address = args.planFacts.addressBySourceId[over.sourceID];
   if (address === undefined) return undefined;
   return {
      address,
      name: over.sourceName,
      coordinate,
      parentIncremental:
         args.incremental?.declarations[over.sourceID]?.incremental === true,
      entry: args.builtEntries[address],
      builtThisRun: args.builtThisRun?.has(address) ?? false,
   };
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

/** `render`, handing a moved upstream's tables to the drift handler. */
function reportingDrift<T>(environment: BuildEnvironment, render: () => T): T {
   try {
      return render();
   } catch (err) {
      if (err instanceof SchemaIncompatibleError && err.handles.length > 0) {
         environment.reportIncompatible?.(err.handles);
      }
      throw err;
   }
}
