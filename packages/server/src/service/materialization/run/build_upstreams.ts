// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a build needs from the plan about its stored upstreams: the plan's
 * facts ({@link ChainedPlanFacts}, {@link chainedPlanFacts}) and the manifest
 * the compiler renders over ({@link manifestForCompiler},
 * {@link upstreamReuseFromManifest}); which upstream entries are current for
 * their declared tier, and the refusal of one a host supplied at the wrong
 * tier ({@link HostSuppliedWrongTierError}); the environment surface a build
 * reads ({@link BuildEnvironment}); the destination a declared `storage=`
 * builds into; and a run's results named package-relative.
 */

import { declaredDestination, storageWritable } from "../domain/placement";
import { underCompilerKeys } from "../domain/binding";
import { readsTable } from "../domain/table_name";
import {
   buildSQLOf,
   compiledSourceIDOf,
   compilerBuildId,
   computeSourceEntityId,
   declaredStorage,
   generatedNameOwner,
   type TableFolds,
} from "./run_plan";
import { type MaterializePlan } from "../domain/table_plans";
import type { ChainBuildRequest, ChainBuildText } from "../domain/chain_build";
import type { Connection, PersistSource } from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import { ChainedShapeNotCarriedError } from "../../../errors";
import { logger } from "../../../logger";
import { ManifestEntry, RefusedSource, SourceFailure } from "../domain/types";
import { getPersistStorageMode } from "../../../config";
import { groupAliasesByName, planSourceIds } from "../domain/source_aliases";
import type { ApiConnection } from "../../model";
import { packageRelativeText } from "../../../package_relative_text";
import type { UpstreamSchemaReadings } from "../domain/upstream_schemas";

/**
 * What a build needs from the plan about stored tables: which reached sources
 * are stored and whether their tables are at hand. These are the PLAN's facts,
 * not a definition's: `#@ persist` is inherited and `extend` keeps the SQL, so
 * an extension or rename shares the base's table, and a definition does not
 * say which name wrote the annotation. Every name whose SQL hashes to one
 * content address is that table (`aliasesBySourceName`).
 */
export interface ChainedPlanFacts {
   /**
    * Every persist source's `sourceID`, inheritors and renames included. By id
    * because a name is not unique across a package's models.
    */
   persistSourceIds: ReadonlySet<string>;
   /** The names sharing one table, keyed by each of them (groupAliasesByName). */
   aliasesBySourceName: Record<string, string[]>;
   /**
    * Each source's content address (its manifest key) by `sourceID`. Absent
    * when the source's SQL could not be rendered.
    */
   addressBySourceId: Record<string, string | undefined>;
   /**
    * Each source's key as the compiler computes it (`compilerBuildId`): the
    * address without the publisher's folded `storage=` destination, so it
    * differs only for a source that has one, whose entry the compiler finds
    * only under it.
    */
   compilerKeyBySourceId: Record<string, string | undefined>;
   /**
    * The plan's own sourceIDs for a source name at a content address
    * ({@link planSourceIds}), so a chained build rebinds an upstream only where
    * the downstream's model means that source. Absent without a plan, which
    * leaves upstreams matched by name.
    */
   sourceIDsFor?: (sourceName: string, sourceEntityId: string) => string[];
   /**
    * Where the plan's sources at each content address live NOW: a storage
    * destination's name, or null for the source's own warehouse (a `storage=`
    * source counts as null while `PERSIST_STORAGE_MODE` is off). Read from the
    * run's instruction for the source when there is one (a host can imply a
    * destination the annotation does not declare), else from `storage=`. A
    * destination that is the source's own connection counts as null: the same
    * database (see {@link normalizedTier}).
    * The address does not include the destination, so a source that moved
    * tier keeps its address, and an entry for it may describe the table it had
    * before the move. {@link upstreamEntryIsCurrent} reads this. Absent without
    * a plan, which leaves every entry trusted.
    */
   declaredTiersByAddress?: Record<string, readonly (string | null)[]>;
   /** The plan's source names at each content address, for messages. */
   namesByAddress?: Record<string, readonly string[]>;
   /**
    * Whether the host instructed this run (orchestrated), supplying its
    * manifest and references. A host-supplied upstream entry at the wrong tier
    * is then refused by name ({@link HostSuppliedWrongTierError}): a host
    * that hands a build another tier's table (two workers with separate
    * stores sharing one config) is a case the host must prevent. A
    * self-instructed run's build that reads one fails its strict render.
    * Absent means self-instructed.
    */
   hostInstructed?: boolean;
   /**
    * The plan's `#(materialize)` plans by sourceID. A `storage=` extension
    * table among them (one with `buildsOver.overStored`) builds over its
    * parent's stored table from its own chain, not by lifting author text.
    */
   materializePlans?: Readonly<Record<string, MaterializePlan>>;
}

/** A build with no plan to consult: nothing is known to be stored. */
export const NO_PLAN_FACTS: ChainedPlanFacts = {
   persistSourceIds: new Set(),
   aliasesBySourceName: {},
   addressBySourceId: {},
   compilerKeyBySourceId: {},
};

/**
 * The run's manifest as the compiler must see it: the entry of a source whose
 * address folds in its destination also under the compiler's own key, since `persistedTableFor` looks it up by
 * that key and would otherwise inline (or strictly miss) the upstream. Only for
 * the compiler's renders; the ledger stays keyed by address.
 *
 * Sources folded apart share one compiler key. `reached`, the persisted
 * sources the rendered source can read (by sourceID, through every upstream
 * the render may inline: {@link persistedReach}), decides which of them may
 * fill it: a key a reached source maps to holds only a reached source's own
 * entry, or none, never a same-SQL sibling's, whose table may be in another
 * tier or of another age. Absent, the first source with an entry fills it.
 */
export function manifestForCompiler(
   full: Manifest["buildManifest"],
   planFacts: Pick<
      ChainedPlanFacts,
      "addressBySourceId" | "compilerKeyBySourceId"
   >,
   reached?: ReadonlySet<string>,
): Manifest["buildManifest"] {
   const entries = { ...full.entries };
   let changed = false;
   // A reached source's key holds a reached source's own table, or nothing:
   // a sibling's entry under it (filed below, or a sibling whose address IS
   // the key) is never read in its place. Every reached source with an entry
   // fills its key first; only a key none of them filled is cleared.
   const claimed = new Set<string>();
   if (reached !== undefined) {
      const filled = new Set<string>();
      for (const sourceID of reached) {
         const key = planFacts.compilerKeyBySourceId[sourceID];
         const address = planFacts.addressBySourceId[sourceID];
         if (key === undefined || address === undefined) continue;
         claimed.add(key);
         if (!(address in full.entries)) continue;
         filled.add(key);
         if (key !== address) {
            entries[key] = full.entries[address];
            changed = true;
         }
      }
      for (const key of claimed) {
         if (!filled.has(key) && key in entries) {
            delete entries[key];
            changed = true;
         }
      }
   }
   const keyed = changed ? { ...full, entries } : full;
   // Every other key: the first source with an entry fills it, as every
   // manifest handed to Malloy is filed (underCompilerKeys).
   return underCompilerKeys(
      keyed,
      Object.entries(planFacts.addressBySourceId).flatMap(
         ([sourceID, address]) => {
            const key = planFacts.compilerKeyBySourceId[sourceID];
            return address === undefined || key === undefined
               ? []
               : [[address, key] as const];
         },
      ),
      claimed,
   );
}

/**
 * How a build read the stored tables its source depends on, decided from the
 * compiler's own lookup: an upstream was read from its table exactly when its
 * address is in `substituted` (the manifest the build SQL was rendered with),
 * whatever the entry is called (a by-reference entry has no `sourceName`).
 *
 * - `inManifestOnly`: upstreams held only by a `storage=` destination, which
 *   `manifestExcludingStorage` drops. Parents to stack on for a `storage=`
 *   build; a database one that read one would recompute it (`inlined`).
 * - `missing`: upstreams the SQL reads that no manifest holds, from the
 *   compiler (`compilerMissing`, {@link compilerMissingUpstream}) rather than
 *   the walk, which both over-reaches (joins the query never reads) and
 *   under-reaches (joins declared on a stop itself). A walk stop no manifest
 *   holds and the compiler did not miss is not read at all.
 *
 * A stop at `rootAddress` is the table being built (an extension shares its
 * base's address), not an upstream. An upstream with no address is skipped;
 * a build that truly reads it fails on its own render.
 *
 * - `inlined`: why the build SQL would recompute an upstream from its
 *   definition rather than read its table. No build may: the caller refuses
 *   the build when any is listed ({@link refuseInlinedUpstreams}). A
 *   `storage=` build lists only an upstream read through its definition: its
 *   stored parents are read by stacking on them, not by this SQL.
 *
 * `sqlInlinesStored` is a floor under the walk: it compares the build SQL with
 * one rendered with every stored entry available, catching a join declared on
 * a stored stop itself (`daily` with `join_one: c is daily_counts`, the latter
 * in storage) that only the compiler reaches. It gates a `storage=` build's
 * stack-on-parent attempt, and inlines an upstream for a database one.
 *
 * `upstreamReuse` is `reused` whenever the build reads a stored upstream;
 * nothing writes `recomputed` (retained in the schema for older entries).
 */
export function upstreamReuseFromManifest(params: {
   reached: readonly { name: string; sourceID: string }[];
   addressBySourceId: Record<string, string | undefined>;
   /** The manifest the compiler substituted from (storage entries excluded). */
   substituted: Record<string, unknown>;
   /** The run's whole manifest, storage entries included. */
   full: Record<string, unknown>;
   /** The run's entries so far, by id, for a storage entry's destination. */
   builtEntries: Record<string, ManifestEntry>;
   tier: "database" | "storage";
   /** The build SQL differs from the SQL rendered with every stored entry available. */
   sqlInlinesStored: boolean;
   /** The persisted source the SQL reads that no manifest holds, per the compiler's strict render. */
   compilerMissing: readonly { name: string; sourceID: string }[];
   /** The content address of the source being built; a stop sharing it is its own table. */
   rootAddress?: string;
   /**
    * The upstreams left out because their table moved since its build, by
    * address: why, for one the build reads, else undefined (not read).
    */
   moved?: ReadonlyMap<string, string | undefined>;
   /**
    * The source is a persisted view's or named query's build source, whose
    * companion blanks every source-level `#@` line: the compiler substitutes
    * no upstream's table, so each upstream would be read through its
    * definition, whatever the manifest holds (`inlined`). The plan builds
    * such a source over its upstreams' tables or refuses it, so this is an
    * invariant's input, never a path.
    */
   readsDefinitions?: boolean;
}): {
   fields: Pick<ManifestEntry, "upstreamReuse">;
   inManifestOnly: string[];
   missing: string[];
   inlined: string[];
} {
   const inManifestOnly: string[] = [];
   const missing: string[] = [];
   const inlined: string[] = [];
   const database = params.tier === "database";
   let upstreams = 0;
   for (const { name, sourceID } of params.reached) {
      const address = params.addressBySourceId[sourceID];
      if (address !== undefined && address === params.rootAddress) continue;
      // No address: had this build read it, its own render would have failed.
      if (address === undefined) continue;
      upstreams++;
      if (params.readsDefinitions) {
         const why =
            `persisted upstream '${name}' is not read from its table: a ` +
            `persisted view or query builds from its source's definition`;
         if (!inlined.includes(why)) inlined.push(why);
         continue;
      }
      if (address in params.substituted) continue;
      if (params.moved?.has(address)) {
         // Its table moved since its build: the build refused it if read
         // (refuseMovedRead), else it is unused.
         const why = params.moved.get(address);
         if (why !== undefined && database) inlined.push(why);
         continue;
      }
      if (address in params.full) {
         // Storage-held, excluded from the warehouse manifest. Read only if the
         // compare says a stored table was inlined; otherwise an unused join.
         if (!params.sqlInlinesStored) continue;
         inManifestOnly.push(name);
         if (database) {
            const destination =
               params.builtEntries[address]?.storageDestinationName;
            inlined.push(
               destination
                  ? `persisted upstream '${name}' is materialized in storage ` +
                       `destination '${destination}', which a warehouse build cannot read`
                  : // A warehouse entry for a source now declared `storage=`:
                    // the table from before it moved, or a stored table a
                    // reference did not label. Neither is read here.
                    `persisted upstream '${name}' is declared in a storage ` +
                       `destination, and its manifest entry is not a table a ` +
                       `warehouse build can read`,
            );
         }
         continue;
      }
      // In no manifest and not missed by the compiler: a join the query never
      // reads.
   }
   // Refused by the caller as missing, before anything is written.
   for (const { name } of params.compilerMissing) missing.push(name);
   // A stored table was inlined and no walk stop accounts for it: a join on a
   // stored stop's own definition, which only the compiler reached.
   if (
      database &&
      params.sqlInlinesStored &&
      inlined.length === 0 &&
      inManifestOnly.length === 0
   ) {
      inlined.push(
         `the build SQL inlines a stored table reached through a refinement ` +
            `declared on a stored upstream (a join the walk does not follow)`,
      );
   }
   const fields: Pick<ManifestEntry, "upstreamReuse"> =
      upstreams === 0 &&
      !params.sqlInlinesStored &&
      params.compilerMissing.length === 0
         ? {}
         : { upstreamReuse: "reused" };
   return { fields, inManifestOnly, missing, inlined };
}

/**
 * The invariant every build holds: none recomputes a persisted upstream from
 * its definition. The plan refuses each shape that would (a persisted result
 * not over its upstreams' tables, a database build over a stored table), and
 * a strict render or the moved-upstream check refuses the rest, so `inlined`
 * ({@link upstreamReuseFromManifest}) is expected empty. Should one get
 * through, the build is refused loudly (logged with `reasonCode:
 * upstream_inlined`, failed as `chained_shape_not_carried`), never written.
 */
export function refuseInlinedUpstreams(
   sourceName: string,
   inlined: readonly string[],
): void {
   if (inlined.length === 0) return;
   logger.error(
      "A build would recompute a persisted upstream from its definition; refused",
      { sourceName, reasonCode: "upstream_inlined", reasons: inlined },
   );
   throw new ChainedShapeNotCarriedError(
      `Failed to materialize source '${sourceName}': ${inlined.join("; ")}, ` +
         `and a build never recomputes a persisted upstream from its ` +
         `definition`,
   );
}

/**
 * A compiler strict manifest miss, recognized by its `code` (the contract),
 * with the `sourceID` parsed from the message when it has the shape the
 * real-compiler spec pins. A reworded message yields a miss of an unnamed
 * source, not no miss. The id belongs on the error object, which Malloy does
 * not put there.
 */
export function strictMissSourceId(
   err: unknown,
): { sourceID?: string } | undefined {
   const e = err as { code?: unknown; message?: unknown } | null;
   if (e?.code !== "runtime-manifest-strict-miss") return undefined;
   const sourceID =
      typeof e.message === "string"
         ? /Persisted source '([^']+)' not found in manifest/.exec(
              e.message,
           )?.[1]
         : undefined;
   return { sourceID };
}

/**
 * The name a reason gives a source: a name only a `#(materialize)` source's
 * generated files declare (`<src>__mat`, in a file the package never shows)
 * by the author's source it stands for ({@link generatedNameOwner}), any
 * other as it is.
 */
export function presentedSourceName(
   name: string,
   planFacts: Pick<ChainedPlanFacts, "materializePlans">,
): string {
   return (
      generatedNameOwner(
         name,
         Object.values(planFacts.materializePlans ?? {}),
      ) ?? name
   );
}

/**
 * The persisted source this build's SQL reads that no manifest holds, found by
 * rendering with the run's whole manifest and `strict: true`, so only sources
 * the SQL actually reads can miss. The compiler stops at the first miss, so
 * this names at most one source per build.
 */
export function compilerMissingUpstream(
   persistSource: PersistSource,
   fullManifest: Manifest["buildManifest"],
   sourceNameById: ReadonlyMap<string, string>,
): { name: string; sourceID: string }[] {
   try {
      buildSQLOf(persistSource, { ...fullManifest, strict: true });
      return [];
   } catch (err) {
      const miss = strictMissSourceId(err);
      if (miss === undefined) throw err;
      const sourceID = miss.sourceID ?? "";
      // A reason names a source, never the server's path to its model file.
      const name =
         sourceNameById.get(sourceID) ??
         (sourceID
            ? sourceID.split("@")[0]
            : "a persisted source the SQL reads");
      return [{ name, sourceID }];
   }
}

/**
 * Derive {@link ChainedPlanFacts} from the compiled plan.
 *
 * Each source is addressed by `computeSourceEntityId`, which
 * `build_targets_address_equality.spec.ts` pins equal to `target.buildId` for
 * a source whose address folds in no destination. A folded `storage=`
 * destination moves the address but not the key the compiler files the table
 * under, so the compiler's key is kept beside it (`compilerKeyBySourceId`, read
 * by `manifestForCompiler`).
 */
export function chainedPlanFacts(
   tables: Readonly<Record<string, { source: PersistSource }>>,
   connectionDigests: Record<string, string>,
   /** The plan's table folds (RunPlan.tableFolds). */
   tableFolds: TableFolds | undefined,
   /** The plan's upstream readings (RunPlan.upstreamSchemas). */
   upstreamSchemas: UpstreamSchemaReadings | undefined,
   /**
    * The run's instruction for a source, when it has one: its `destination`
    * decides the source's current tier over the `storage=` annotation, since a
    * host can imply one (a source on a database connection built into that
    * database). Absent outside an instructed run.
    */
   instructionFor?: (
      sourceID: string,
      sourceEntityId: string,
   ) => { destination?: string } | undefined,
): ChainedPlanFacts {
   const planSources: { name: string; sourceEntityId?: string }[] = [];
   const planSourcesById: Record<
      string,
      { name: string; sourceEntityId?: string }
   > = {};
   const addressBySourceId: Record<string, string | undefined> = {};
   const compilerKeyBySourceId: Record<string, string | undefined> = {};
   const declaredTiersByAddress: Record<string, (string | null)[]> = {};
   const namesByAddress: Record<string, string[]> = {};
   const storageOn = storageWritable(getPersistStorageMode());
   for (const [sourceID, { source }] of Object.entries(tables)) {
      let sourceEntityId: string | undefined;
      try {
         sourceEntityId = computeSourceEntityId(
            source,
            connectionDigests,
            undefined,
            tableFolds,
            upstreamSchemas,
         );
         compilerKeyBySourceId[sourceID] = compilerBuildId(source);
      } catch {
         // Unrenderable (unbound parameter, given): no address to group by.
         // The eligibility gate refuses it on its own path.
      }
      planSources.push({ name: source.name, sourceEntityId });
      planSourcesById[sourceID] = { name: source.name, sourceEntityId };
      addressBySourceId[sourceID] = sourceEntityId;
      // A `#(materialize)` table under the sourceID its build source was
      // compiled under too, which a downstream compiled over the table reads
      // it by (a view or rollup re-rooting it), so its chained build finds
      // the upstream's address and key as for any other.
      const compiledID = compiledSourceIDOf(source);
      if (compiledID !== undefined && !(compiledID in tables)) {
         planSourcesById[compiledID] = { name: source.name, sourceEntityId };
         addressBySourceId[compiledID] = sourceEntityId;
         compilerKeyBySourceId[compiledID] = compilerKeyBySourceId[sourceID];
      }
      if (sourceEntityId !== undefined) {
         let tier: string | null = null;
         if (storageOn) {
            const instructed = instructionFor?.(sourceID, sourceEntityId);
            if (instructed) {
               tier = instructed.destination ?? null;
            } else {
               try {
                  tier = declaredStorage(source) ?? null;
               } catch {
                  // Unreadable annotations: the database reading, as the build does.
               }
            }
            tier = normalizedTier(tier, source.connectionName);
         }
         const tiers = (declaredTiersByAddress[sourceEntityId] ??= []);
         if (!tiers.includes(tier)) tiers.push(tier);
         (namesByAddress[sourceEntityId] ??= []).push(source.name);
      }
   }
   return {
      // With each `#(materialize)` table's compiled sourceID, as above.
      persistSourceIds: new Set(Object.keys(planSourcesById)),
      aliasesBySourceName: groupAliasesByName(planSources),
      addressBySourceId,
      compilerKeyBySourceId,
      sourceIDsFor: planSourceIds(planSourcesById),
      declaredTiersByAddress,
      namesByAddress,
   };
}

/**
 * Whether a manifest entry describes the table its source is declared to have
 * NOW (pinned by build_upstreams.spec.ts). An entry's tier is its
 * `storageDestinationName`, or null for a warehouse table. A source moved to
 * or from `storage=` keeps its content address, so a carried or referenced
 * entry can be the table from before the move: a warehouse table for a source
 * now stored, or a stored table for a source now database or in another
 * destination. Such an entry is stale and is never used as an upstream: not
 * rendered into warehouse SQL, and not stacked on. An address the plan does
 * not know, or a run with no plan, keeps every entry.
 */
export function upstreamEntryIsCurrent(
   address: string,
   entry: Pick<ManifestEntry, "storageDestinationName" | "connectionName">,
   planFacts: Pick<ChainedPlanFacts, "declaredTiersByAddress">,
): boolean {
   const tiers = planFacts.declaredTiersByAddress?.[address];
   if (!tiers || tiers.length === 0) return true;
   return tiers.includes(entryTier(entry));
}

/**
 * A tier as {@link upstreamEntryIsCurrent} compares it: a storage destination's
 * name, or null for the source's own warehouse. A destination that IS the
 * source's own connection is null too: a host that implies a destination for a
 * source on a database connection builds into that same database, so its table
 * is the warehouse table, not one that moved.
 */
export function normalizedTier(
   destination: string | null | undefined,
   connectionName: string | undefined,
): string | null {
   return destination && destination !== connectionName ? destination : null;
}

/** An entry's tier (see {@link normalizedTier}). */
export function entryTier(
   entry: Pick<ManifestEntry, "storageDestinationName" | "connectionName">,
): string | null {
   return normalizedTier(entry.storageDestinationName, entry.connectionName);
}

/**
 * The entries a chained `storage=` build may stack on: only those at the tier
 * their source is declared on now (see {@link upstreamEntryIsCurrent}).
 */
export function currentUpstreamEntries(
   builtEntries: Record<string, ManifestEntry>,
   planFacts: Pick<ChainedPlanFacts, "declaredTiersByAddress">,
): Record<string, ManifestEntry> {
   return Object.fromEntries(
      Object.entries(builtEntries).filter(([address, entry]) =>
         upstreamEntryIsCurrent(address, entry, planFacts),
      ),
   );
}

/**
 * A host-instructed build refused because the host supplied an upstream's
 * entry at the wrong tier for the upstream as declared now. Deterministic:
 * the same instructions fail the same way, so `retryable: false`
 * (build_failure_class.ts), and the host sees why.
 */
export class HostSuppliedWrongTierError extends Error {
   readonly missing: readonly string[];

   constructor(
      sourceName: string,
      upstreams: readonly { name: string; detail: string }[],
   ) {
      super(
         `Failed to materialize source '${sourceName}': the host supplied an ` +
            `upstream entry this build cannot use: ` +
            upstreams.map((u) => u.detail).join("; ") +
            `. An instructed build refuses rather than recomputing that ` +
            `upstream from raw; supply the entry for the table the upstream ` +
            `is declared to have now (with its storageDestinationName).`,
      );
      this.name = "HostSuppliedWrongTierError";
      this.missing = upstreams.map((u) => u.name);
   }
}

/**
 * A chained `storage=` build could not stack on its parents because its
 * package serves a model older than its files (its last reload did not
 * compile): the build text is read from those files, which the plan's content
 * addresses do not describe. A shape not carried, with its own code, so the
 * caller refuses it as such.
 */
export class ChainedPackageStaleError extends ChainedShapeNotCarriedError {
   /** The code a log or metric carries for it (errorCodeOf). */
   readonly reason = "package_stale" as const;

   constructor(detail: string) {
      super(detail);
      this.name = "ChainedPackageStaleError";
   }
}

/** The phrase every wrong-tier detail carries, for a caller that classifies one. */
export const WRONG_TIER_MARKER = "supplied at the wrong tier";

function tierLabel(tier: string | null): string {
   return tier === null ? "its warehouse" : `storage destination '${tier}'`;
}

/**
 * Why an upstream's entry is not used because it is at the wrong tier: where
 * the entry is, and where the upstream is declared now. An unlabeled entry (no
 * `storageDestinationName`) is a warehouse table or a reference that does not
 * say where it lives; until a host labels its references that is every
 * reference to a stored table, so the message says so rather than calling it
 * the table from before a move.
 */
export function wrongTierDetail(
   name: string,
   address: string,
   entry: Pick<ManifestEntry, "storageDestinationName" | "connectionName">,
   planFacts: Pick<ChainedPlanFacts, "declaredTiersByAddress">,
): string {
   const declared = planFacts.declaredTiersByAddress?.[address] ?? [];
   const where = declared.length
      ? declared.map(tierLabel).join(" or ")
      : "another tier";
   const tier = entryTier(entry);
   const at =
      tier === null
         ? "a warehouse table, or a reference that does not say where it lives"
         : `a table in ${tierLabel(tier)}`;
   return (
      `'${name}' is declared in ${where}, but its manifest entry is ${at}: ` +
      `${WRONG_TIER_MARKER}, so this build does not use it`
   );
}

/**
 * The narrow environment surface the build path needs, plus the environment
 * root (for a plain-DuckDB destination's file path).
 *
 * The two lookups are separate on purpose, and that is the security property:
 * `getApiConnection` resolves the SOURCE read from, `getStorageDestination`
 * where the table is WRITTEN. They read disjoint lists, so a `storage=` build
 * can never be steered into a connection by naming it.
 */
export interface BuildEnvironment {
   getApiConnection(connectionName: string): ApiConnection;
   getStorageDestination(destinationName: string): ApiConnection;
   getEnvironmentPath(): string;
   /** For the usage record a metered build emits. */
   getEnvironmentName(): string;
   /**
    * The package's text for a chained `storage=` build (the host port's
    * `chainBuildText`). Absent, a chained build cannot stack on its parents
    * and is refused.
    */
   chainBuildText?(request: ChainBuildRequest): ChainBuildText;
   /**
    * Whether a `storage=` build's BigQuery SQL reads only strings compared
    * byte-exact, read from the catalog as an answer's check reads it (the
    * host port's; serve/admission_lookup.ts), for its column contract. Absent,
    * its strings are recorded compared `none`.
    */
   bigqueryBuildCompareBinary?(args: {
      connection: Connection;
      source: PersistSource;
      sql: string;
   }): Promise<boolean>;
   /**
    * The root of the package being built, which the build's reads of author
    * files stay inside. Absent in test doubles: those reads are then confined
    * to the files the model compiled from alone.
    */
   getPackagePath?(): string;
   /**
    * The drift handler, for a build that found an upstream's table not as
    * its plan recorded it (SchemaIncompatibleError): the package reads its
    * tables and derives its plan again. Absent in test doubles.
    */
   reportIncompatible?(handles: readonly string[]): void;
}

/**
 * `environment` with the package's `chainBuildText`, for one run of that package. The environment
 * itself is left as it is.
 */
export function withChainBuildText<E extends BuildEnvironment>(
   environment: E,
   pkg: {
      chainBuildText(request: ChainBuildRequest): ChainBuildText;
      bigqueryBuildCompareBinary?: BuildEnvironment["bigqueryBuildCompareBinary"];
      getPackagePath(): string;
      reportIncompatible?(handles: readonly string[]): void;
   },
   /**
    * Whether the package serves a model older than its files (a reload that
    * did not compile), asked at each build: its files are not what the plan's
    * addresses describe, so no chained build is written from them.
    */
   isStale: () => boolean = () => false,
): E {
   const scoped = Object.create(environment) as E;
   const packagePath = pkg.getPackagePath();
   scoped.getPackagePath = () => packagePath;
   scoped.chainBuildText = (request) =>
      isStale()
         ? {
              declined:
                 `its package ${PACKAGE_STALE_MARKER}: its last reload did ` +
                 `not compile, so the files a chained build is written from ` +
                 `are not the model this build was planned from`,
              code: "package_stale",
           }
         : pkg.chainBuildText(request);
   if (pkg.bigqueryBuildCompareBinary) {
      scoped.bigqueryBuildCompareBinary = pkg.bigqueryBuildCompareBinary;
   }
   if (pkg.reportIncompatible) {
      scoped.reportIncompatible = (handles) => pkg.reportIncompatible!(handles);
   }
   return scoped;
}

/** The phrase a stale package's recompute reason carries, for a reader that classifies one. */
export const PACKAGE_STALE_MARKER = "serves a model older than its files";

/**
 * The build manifest for a `storage=` build, with storage-held entries removed.
 * The build runs in the source warehouse, so dropping them makes the compiler
 * INLINE the upstream (or strictly miss) instead of emitting a cross-engine
 * reference. Database entries keep their dialect-quoted table path so the
 * downstream FROM resolves a case-preserved table on a case-folding engine.
 * Preserves `strict`.
 */
export function manifestExcludingStorage(
   manifest: Manifest,
   builtEntries: Record<string, ManifestEntry>,
   planFacts: Pick<ChainedPlanFacts, "declaredTiersByAddress"> = NO_PLAN_FACTS,
): Manifest["buildManifest"] {
   const reduced = new Manifest();
   reduced.strict = manifest.strict;
   // Reuse the already dialect-quoted names, not the raw physical names.
   const quoted = manifest.buildManifest.entries;
   for (const [id, entry] of Object.entries(builtEntries)) {
      // A warehouse entry for a source now declared `storage=` is either the
      // table from before the move or a stored table a reference did not label
      // (a reference that omits `storageDestinationName` reads as a warehouse
      // table): either way its name must never reach warehouse SQL.
      if (
         !entry.storageDestinationName &&
         quoted[id] &&
         upstreamEntryIsCurrent(id, entry, planFacts)
      ) {
         if (
            planFacts.declaredTiersByAddress &&
            !(id in planFacts.declaredTiersByAddress)
         ) {
            // Kept: the plan does not know this address, so nothing
            // says whether it is current. Logged, since an unlabeled reference
            // here could still be a stored table's name.
            logger.debug(
               "Kept an unlabeled manifest entry for an address the plan does not know",
               { sourceEntityId: id, sourceName: entry.sourceName },
            );
         }
         reduced.update(id, { tableName: quoted[id].tableName });
      }
   }
   return reduced.buildManifest;
}

/**
 * The `storage=` destination a source DECLARES, or undefined. Independent of
 * `PERSIST_STORAGE_MODE`, so a `storage=` source is recognizable even when the
 * tier is off.
 */

/**
 * The EFFECTIVE `storage=` destination name for a build, or undefined for the
 * database path. Managed-tier aliases are resolved by the host and arrive on
 * the wire instruction's `destination`, never here.
 *
 * `PERSIST_STORAGE_MODE=off` returns undefined regardless, a kill switch that
 * never fails a package (the ignored `storage=` is a package warning).
 * Undefined does NOT mean "build in the database tier" for a source that declared
 * `storage=`: `deriveSelfInstructions` skips it so it serves live; see
 * {@link declaredStorage}.
 */
export function resolveStorageDestination(
   persistSource: PersistSource,
): string | undefined {
   return declaredDestination(
      declaredStorage(persistSource),
      getPersistStorageMode(),
   );
}

/**
 * A run's results with every message a reader of the run sees (a failure's
 * reason, an entry's error or recompute reason, a refusal's message) naming
 * the package's files package-relative, never by where the server keeps
 * them: a compile error names its file by the URL it was loaded from.
 */
export function packageRelativeRun(
   run: {
      entries: Record<string, ManifestEntry>;
      failures: Record<string, SourceFailure>;
      refused: Record<string, RefusedSource>;
   },
   packagePath: string,
): {
   entries: Record<string, ManifestEntry>;
   failures: Record<string, SourceFailure>;
   refused: Record<string, RefusedSource>;
} {
   const rel = (text: string) => packageRelativeText(text, packagePath);
   const each = <T>(
      record: Record<string, T>,
      map: (value: T) => T,
   ): Record<string, T> =>
      Object.fromEntries(Object.entries(record).map(([k, v]) => [k, map(v)]));
   return {
      entries: each(run.entries, (e) => ({
         ...e,
         ...(typeof e.error === "string" ? { error: rel(e.error) } : {}),
      })),
      failures: each(run.failures, (f) => ({ ...f, reason: rel(f.reason) })),
      refused: each(run.refused, (r) =>
         typeof r.message === "string" ? { ...r, message: rel(r.message) } : r,
      ),
   };
}

/**
 * The stored upstream tables a database build's SQL reads, as
 * `ManifestEntry.builtFrom` records them: each entry of the run's manifest,
 * other than the table being built (`rootAddress`), whose table the SQL reads
 * by its whole name ({@link readsTable}), with its entry's `builtAt`. Sorted
 * by address. Undefined when it reads none.
 */
export function builtFromOf(
   buildSQL: string,
   /** Absent for a build given no manifest (a direct call): it read none. */
   manifest: Manifest["buildManifest"] | undefined,
   builtEntries: Readonly<Record<string, ManifestEntry>>,
   rootAddress: string | undefined,
): ManifestEntry["builtFrom"] {
   return builtFromAddresses(
      Object.entries(manifest?.entries ?? {}).flatMap(
         ([address, { tableName }]) =>
            address !== rootAddress &&
            tableName &&
            readsTable(buildSQL, tableName)
               ? [address]
               : [],
      ),
      builtEntries,
   );
}

/**
 * `ManifestEntry.builtFrom` for the stored tables at `addresses` a build
 * read (a `storage=` build that read its upstreams' tables in its
 * destination names them itself), each with its entry's `builtAt`. Sorted by
 * address. Undefined when none has a table.
 */
export function builtFromAddresses(
   addresses: readonly string[],
   builtEntries: Readonly<Record<string, ManifestEntry>>,
): ManifestEntry["builtFrom"] {
   const out: NonNullable<ManifestEntry["builtFrom"]> = [];
   for (const address of new Set(addresses)) {
      const entry = builtEntries[address];
      if (!entry?.physicalTableName) continue;
      out.push({
         sourceEntityId: address,
         physicalTableName: entry.physicalTableName,
         ...(entry.storageDestinationName
            ? { storageDestinationName: entry.storageDestinationName }
            : {}),
         ...(entry.builtAt ? { builtAt: entry.builtAt } : {}),
         // An incremental upstream's boundary as read: an extension's range
         // ends no later (incremental_upstream.ts).
         ...(entry.ledger
            ? { coveredThrough: entry.ledger.coveredThrough }
            : {}),
      });
   }
   out.sort((a, b) =>
      a.sourceEntityId < b.sourceEntityId
         ? -1
         : a.sourceEntityId > b.sourceEntityId
           ? 1
           : 0,
   );
   return out.length > 0 ? out : undefined;
}
