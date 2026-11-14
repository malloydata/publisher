// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   generatedNameOwner,
   type MaterializePlan,
} from "../plan/build_file_generator";
import type { ChainBuildRequest, ChainBuildText } from "../domain/chain_build";
import type { PersistSource } from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import { MaterializationEligibilityError } from "../../../errors";
import { logger } from "../../../logger";
import { malloyGivenToApi, type MalloyGiven } from "../../given";
import {
   recordStaleUpstreamEntry,
   recordStorageTableRetained,
} from "../../../materialization_metrics";
import {
   BuildInstruction,
   isLegacyFailedEntry,
   MaterializationStatus,
   ManifestEntry,
   RefusedSource,
   SourceFailure,
} from "../domain/types";
import { errMessage } from "../../../utils";
import {
   buildSQLOf,
   computeSourceEntityId,
   deriveAnnotationFields,
   compilerBuildId,
} from "../plan/build_plan";
import { declaredStorage } from "../plan/persist_source_fields";
import { type QueryContext, type QueryMetadata } from "../../query_metadata";
import type { components } from "../../../api";
import { getPersistStorageMode } from "../../../config";
import { resolvePartitionColumns } from "../plan/persist_partition";
import { escapeSQL } from "../../connection";
import { type ServeShapeGiven } from "../serve/materialization_serve_transform";
import { groupAliasesByName, planSourceIds } from "../domain/source_aliases";

import type { ApiConnection } from "../../model";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { redactPgSecrets } from "../../../pg_helpers";

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
    * address without the publisher's `partition=` layout, so it differs only
    * for a partitioned source, whose entry the compiler finds only under it.
    */
   compilerKeyBySourceId: Record<string, string | undefined>;
   /**
    * The plan's own sourceIDs for a source name at a content address
    * ({@link planSourceIds}), so a chained build rebinds an upstream only where
    * the downstream's model means that source. Absent without a plan, which
    * leaves upstreams matched by name, as before.
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
    * The address does not include the destination (P12), so a source that moved
    * tier keeps its address, and an entry for it may describe the table it had
    * before the move. {@link upstreamEntryIsCurrent} reads this. Absent without
    * a plan, which leaves every entry trusted, as before.
    */
   declaredTiersByAddress?: Record<string, readonly (string | null)[]>;
   /** The plan's source names at each content address, for messages. */
   namesByAddress?: Record<string, readonly string[]>;
   /**
    * Whether the host instructed this run (orchestrated), supplying its
    * manifest and references. A host-supplied upstream entry at the wrong tier
    * is then refused ({@link HostSuppliedWrongTierError}) rather than
    * recomputed: a host that hands a build another tier's table (two workers
    * with separate stores sharing one config) is a case the host must prevent.
    * A self-instructed run, building from its own manifest, recomputes and
    * warns ({@link noteWrongTierRecompute}). Absent means self-instructed.
    */
   hostInstructed?: boolean;
   /**
    * What a strict build does with a source whose only build recomputes a
    * stored upstream from the warehouse (ms2data/service#6756): `recompute`
    * builds it and reports `upstreamReuse: recomputed`; `refuse` fails it
    * ({@link WarehouseRecomputeRefusedError}). Only a host sets it; absent
    * means `recompute`, and a non-strict build ignores it.
    */
   warehouseReach?: WarehouseReach;
   /**
    * The plan's `#(materialize)` plans by sourceID. A `storage=` extension
    * table among them (one with `buildsOver.overStored`, P10) builds over its
    * parent's stored table from its own chain, not by lifting author text.
    */
   materializePlans?: Readonly<Record<string, MaterializePlan>>;
}

/** See {@link ChainedPlanFacts.warehouseReach}. */
export type WarehouseReach = "recompute" | "refuse";

/** A build with no plan to consult: nothing is known to be stored. */
export const NO_PLAN_FACTS: ChainedPlanFacts = {
   persistSourceIds: new Set(),
   aliasesBySourceName: {},
   addressBySourceId: {},
   compilerKeyBySourceId: {},
};

/**
 * The run's manifest as the compiler must see it: a partitioned source's entry
 * also under the compiler's own key, since `persistedTableFor` looks it up by
 * that key and would otherwise inline (or strictly miss) the upstream. Only for
 * the compare and strict renders; the ledger stays keyed by address.
 */
export function manifestForCompiler(
   full: Manifest["buildManifest"],
   planFacts: Pick<
      ChainedPlanFacts,
      "addressBySourceId" | "compilerKeyBySourceId"
   >,
): Manifest["buildManifest"] {
   let entries = full.entries;
   for (const [sourceID, address] of Object.entries(
      planFacts.addressBySourceId,
   )) {
      const key = planFacts.compilerKeyBySourceId[sourceID];
      if (
         address === undefined ||
         key === undefined ||
         key === address ||
         !(address in entries) ||
         key in entries
      ) {
         continue;
      }
      if (entries === full.entries) entries = { ...entries };
      entries[key] = entries[address];
   }
   return entries === full.entries ? full : { ...full, entries };
}

/**
 * How a build read the stored tables its source depends on, decided from the
 * compiler's own lookup: an upstream was read from its table exactly when its
 * address is in `substituted` (the manifest the build SQL was rendered with),
 * whatever the entry is called (a by-reference entry has no `sourceName`).
 *
 * - `inManifestOnly`: upstreams held only by a `storage=` destination, which
 *   `manifestExcludingStorage` drops. Parents to stack on for a `storage=`
 *   build; recomputed for a colocated one.
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
 * `sqlInlinesStored` is a floor under the walk: it compares the build SQL with
 * one rendered with every stored entry available, catching a join declared on
 * a stored stop itself (`daily` with `join_one: c is daily_counts`, the latter
 * in storage) that only the compiler reaches. When set, the answer is
 * `recomputed`; it also gates a `storage=` build's stack-on-parent attempt.
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
   tier: "colocated" | "storage";
   /** The build SQL differs from the SQL rendered with every stored entry available. */
   sqlInlinesStored: boolean;
   /** The persisted source the SQL reads that no manifest holds, per the compiler's strict render. */
   compilerMissing: readonly { name: string; sourceID: string }[];
   /** The content address of the source being built; a stop sharing it is its own table. */
   rootAddress?: string;
}): {
   fields: Pick<ManifestEntry, "upstreamReuse" | "upstreamRecomputeReason">;
   inManifestOnly: string[];
   missing: string[];
} {
   const inManifestOnly: string[] = [];
   const missing: string[] = [];
   const reasons: string[] = [];
   let upstreams = 0;
   for (const { name, sourceID } of params.reached) {
      const address = params.addressBySourceId[sourceID];
      if (address !== undefined && address === params.rootAddress) continue;
      // No address: had this build read it, its own render would have failed.
      if (address === undefined) continue;
      upstreams++;
      if (address in params.substituted) continue;
      if (address in params.full) {
         // Storage-held, excluded from the warehouse manifest. Read only if the
         // compare says a stored table was inlined; otherwise an unused join.
         if (!params.sqlInlinesStored) continue;
         inManifestOnly.push(name);
         if (params.tier === "colocated") {
            const destination =
               params.builtEntries[address]?.storageDestinationName;
            reasons.push(
               destination
                  ? `persisted upstream '${name}' is materialized in storage ` +
                       `destination '${destination}', which a warehouse build cannot read`
                  : // A warehouse entry for a source now declared `storage=`
                    // (#7087): the table from before it moved, or a stored
                    // table a reference did not label. Neither is read here.
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
   for (const { name } of params.compilerMissing) {
      missing.push(name);
      reasons.push(
         `persisted upstream '${name}' is not in this build's manifest ` +
            `(neither built in this run nor supplied by reference)`,
      );
   }
   // A stored table was inlined and no walk stop accounts for it: a join on a
   // stored stop's own definition, which only the compiler reached.
   if (
      params.sqlInlinesStored &&
      reasons.length === 0 &&
      inManifestOnly.length === 0
   ) {
      reasons.push(
         `the build SQL inlines a stored table reached through a refinement ` +
            `declared on a stored upstream (a join the walk does not follow)`,
      );
   }
   const fields: Pick<
      ManifestEntry,
      "upstreamReuse" | "upstreamRecomputeReason"
   > =
      upstreams === 0 &&
      !params.sqlInlinesStored &&
      params.compilerMissing.length === 0
         ? {}
         : reasons.length === 0
           ? { upstreamReuse: "reused" }
           : {
                upstreamReuse: "recomputed",
                upstreamRecomputeReason:
                   `${reasons.join("; ")}; recomputed from its definition in ` +
                   `the warehouse`,
             };
   return { fields, inManifestOnly, missing };
}

/**
 * A compiler strict manifest miss, recognized by its `code` (the contract),
 * with the `sourceID` parsed from the message when it has today's shape. A
 * reworded message yields a miss of an unnamed source, not no miss. The
 * real-compiler spec pins the wording; the id belongs on the error object
 * (malloydata/malloy#3133).
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
 * an unpartitioned source. A `partition=` layout moves the address but not the
 * key the compiler files the table under, so the compiler's key is kept beside
 * it (`compilerKeyBySourceId`, read by `manifestForCompiler`).
 */
export function chainedPlanFacts(
   sources: Record<string, PersistSource>,
   connectionDigests: Record<string, string>,
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
   const storageOn = getPersistStorageMode() !== "off";
   for (const [sourceID, source] of Object.entries(sources)) {
      let sourceEntityId: string | undefined;
      try {
         sourceEntityId = computeSourceEntityId(source, connectionDigests);
         compilerKeyBySourceId[sourceID] = compilerBuildId(
            source,
            connectionDigests,
         );
      } catch {
         // Unrenderable (unbound parameter, given): no address to group by.
         // The eligibility gate refuses it on its own path.
      }
      planSources.push({ name: source.name, sourceEntityId });
      planSourcesById[sourceID] = { name: source.name, sourceEntityId };
      addressBySourceId[sourceID] = sourceEntityId;
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
                  // Unreadable annotations: the colocated reading, as the build does.
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
      persistSourceIds: new Set(Object.keys(sources)),
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
 * NOW (ms2data/service#7087). An entry's tier is its `storageDestinationName`,
 * or null for a warehouse table. A source moved to or from `storage=` keeps its
 * content address, so a carried or referenced entry can be the table from
 * before the move: a warehouse table for a source now stored, or a stored table
 * for a source now colocated or in another destination. Such an entry is stale
 * and is never used as an upstream: not rendered into warehouse SQL, and not
 * stacked on. An address the plan does not know, or a run with no plan, keeps
 * every entry, as before.
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
 * entry at the wrong tier for the upstream as declared now (ms2data/service#7087,
 * decided 2026-11-12). Deterministic: the same instructions fail the same way,
 * so `retryable: false` (build_failure_class.ts), and the host sees why.
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
 * Log, once per run per upstream, a build that recomputes an upstream from raw
 * because the upstream's entry is at the wrong tier, and count it: a recompute
 * reads the warehouse, which a reader of the entry fields alone would miss.
 * Keyed by the run's plan facts, which a run computes once.
 */
const wrongTierNoted = new WeakMap<object, Set<string>>();
export function noteWrongTierRecompute(params: {
   planFacts: object;
   build: "colocated" | "storage";
   sourceName: string;
   upstream: string;
   detail: string;
}): void {
   let seen = wrongTierNoted.get(params.planFacts);
   if (!seen) {
      seen = new Set();
      wrongTierNoted.set(params.planFacts, seen);
   }
   if (seen.has(params.upstream)) return;
   seen.add(params.upstream);
   recordStaleUpstreamEntry(params.build);
   logger.warn(
      "Recomputing an upstream from raw: its manifest entry is at the wrong tier",
      {
         sourceName: params.sourceName,
         upstream: params.upstream,
         build: params.build,
         reason: params.detail,
      },
   );
}

/** An author model file's text by `file:` URL, or undefined when unreadable. */
export function readAuthorFile(url: string): string | undefined {
   try {
      return readFileSync(fileURLToPath(url), "utf8");
   } catch {
      return undefined;
   }
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
   /** For the usage record a metered build emits; absent in some test doubles. */
   getEnvironmentName?(): string;
   /**
    * The package's text for a chained `storage=` build (the host port's
    * `chainBuildText`). Absent, a chained build cannot stack on its parents
    * and recomputes, or under strict upstreams refuses.
    */
   chainBuildText?(request: ChainBuildRequest): ChainBuildText;
}

/**
 * `environment` with the package's `chainBuildText`, for one run of that
 * package. The environment itself is left as it is.
 */
export function withChainBuildText<E extends BuildEnvironment>(
   environment: E,
   pkg: { chainBuildText?(request: ChainBuildRequest): ChainBuildText },
): E {
   if (!pkg.chainBuildText) return environment;
   const scoped = Object.create(environment) as E;
   scoped.chainBuildText = (request) => pkg.chainBuildText!(request);
   return scoped;
}

/**
 * What a build needs to tag its statements: the package-level metadata layer
 * and the run's context. Constant for the run; a source adds only its name.
 */
export interface BuildQueryMetadata {
   packageMaterialization:
      | components["schemas"]["PackageMaterializationConfig"]
      | null;
   context: QueryContext;
}

/**
 * A connection's two metadata layers: the overridable default and the
 * deployment-enforced properties. Fails open: an unreadable connection costs
 * the tags, never the statement.
 */
export function connectionMetadataLayers(
   environment: BuildEnvironment,
   connectionName: string,
): { default: QueryMetadata | null; enforced: QueryMetadata | null } {
   try {
      const connection = environment.getApiConnection(connectionName);
      return {
         default: connection?.queryMetadata ?? null,
         enforced: connection?.queryMetadataEnforced ?? null,
      };
   } catch (error) {
      // Logged because the lost enforced layer has no metric behind it.
      logger.debug("No query-metadata layers for connection", {
         connectionName,
         error,
      });
      return { default: null, enforced: null };
   }
}

/**
 * Length of the sourceEntityId prefix used when synthesizing staging table
 * names. 12 hex chars is 48 bits of entropy, well inside every dialect's
 * identifier limit (Postgres is the tightest at 63).
 */
export const STAGING_ID_LEN = 12;

/** Staging suffix appended to a table name while it is being built. */
export function stagingSuffix(sourceEntityId: string): string {
   // Drop hyphens so a UUID-shaped id still yields a bare identifier fragment.
   return `_${sourceEntityId.replace(/-/g, "").substring(0, STAGING_ID_LEN)}`;
}

/**
 * Suffix of the name a served table is moved aside to while its rebuilt
 * replacement is renamed into place. The same length as {@link stagingSuffix},
 * so it fits wherever a staging name does, and never equal to it.
 */
export function backupSuffix(sourceEntityId: string): string {
   return `_o${sourceEntityId.replace(/-/g, "").substring(0, STAGING_ID_LEN - 1)}`;
}

/**
 * Physical table name the publisher self-assigns in auto-run mode: the
 * `#@ persist name=<table>` value if present, else the Malloy source name.
 * The author owns quoting the `name=` value for the dialect.
 */
export function selfAssignTableName(persistSource: PersistSource): string {
   return deriveAnnotationFields(persistSource).name || persistSource.name;
}

/**
 * The build manifest for a `storage=` build, with storage-held entries removed.
 * The build runs in the source warehouse, so dropping them makes the compiler
 * INLINE the upstream (or strictly miss) instead of emitting a cross-engine
 * reference. Colocated entries keep their dialect-quoted table path so the
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
      // table): either way its name must never reach warehouse SQL (#7087).
      if (
         !entry.storageDestinationName &&
         quoted[id] &&
         upstreamEntryIsCurrent(id, entry, planFacts)
      ) {
         if (
            planFacts.declaredTiersByAddress &&
            !(id in planFacts.declaredTiersByAddress)
         ) {
            // Kept as before: the plan does not know this address, so nothing
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
export { declaredStorage };

/**
 * The author model's given surface (inheritance through imports already
 * collapsed), for the chained build's transient model. A given whose type
 * cannot be rendered is dropped, not guessed: text reading it then fails to
 * compile, where a guessed type could compile a silently coercing predicate.
 */
export function chainedBuildGivens(
   persistSource: PersistSource,
): ServeShapeGiven[] {
   const out: ServeShapeGiven[] = [];
   for (const given of persistSource._model?.givens?.values() ?? []) {
      const api = malloyGivenToApi(given as unknown as MalloyGiven);
      if (!api.name || !api.type) continue;
      out.push({
         name: api.name,
         type: api.type,
         defaultText: typeof api.default === "string" ? api.default : undefined,
      });
   }
   return out;
}

/**
 * The columns this source's table is laid out by, for a build about to run.
 * Re-resolved (pure over the source and its annotation) rather than threaded
 * from the eligibility gate, so the two agree by contract. A refusal here
 * THROWS: the gate should already have refused it, and building unpartitioned
 * would silently diverge from what the author declared.
 */
export function partitionColumnsForBuild(
   persistSource: PersistSource,
): string[] {
   const resolved = resolvePartitionColumns(
      persistSource,
      deriveAnnotationFields(persistSource),
   );
   if (!resolved.ok) {
      throw new MaterializationEligibilityError({
         reason: resolved.reason,
         message:
            `Source '${persistSource.name}' reached a build with an ` +
            `unusable 'partition=': ${resolved.detail}. The eligibility gate ` +
            `should have refused this before any warehouse work ran.`,
      });
   }
   return resolved.columns;
}

/**
 * The EFFECTIVE `storage=` destination name for a build, or undefined for the
 * colocated path. Managed-tier aliases are resolved by the host and arrive on
 * the wire instruction's `destination`, never here.
 *
 * `PERSIST_STORAGE_MODE=off` returns undefined regardless, a kill switch that
 * never fails a package (the ignored `storage=` is a package warning).
 * Undefined does NOT mean "build colocated" for a source that declared
 * `storage=`: `deriveSelfInstructions` skips it so it serves live; see
 * {@link declaredStorage}.
 */
export function resolveStorageDestination(
   persistSource: PersistSource,
): string | undefined {
   if (getPersistStorageMode() === "off") return undefined;
   return declaredStorage(persistSource);
}

/** Connection-config keys whose string values are credentials to redact. */
const SENSITIVE_KEY =
   /pass(word)?|secret|private_?key|service_?account|access_?key|token|connection_?string|account/i;

/** Collect credential string values (from sensitively-named keys) in a config. */
function collectSensitiveValues(
   value: unknown,
   out: Set<string>,
   seen: WeakSet<object> = new WeakSet(),
): void {
   if (value === null || typeof value !== "object") return;
   // Callers pass a LIVE connection whose driver/pool graph can be cyclic;
   // without this guard the stack overflow would replace the error being
   // redacted. Mirrors the guard in redactSensitive.
   if (seen.has(value)) return;
   seen.add(value);
   if (Array.isArray(value)) {
      for (const v of value) collectSensitiveValues(v, out, seen);
      return;
   }
   for (const [key, v] of Object.entries(value)) {
      if (typeof v === "string" && v.length >= 4 && SENSITIVE_KEY.test(key)) {
         out.add(v);
      } else {
         collectSensitiveValues(v, out, seen);
      }
   }
}

/**
 * How many sources a run built, failed on, and reused, counted from what the
 * build returned (an instruction can be skipped without building).
 * `failures` is the authority on what failed; a legacy entry mirroring a
 * failure (`ManifestEntry.error`) is excluded from the other counts so one
 * lost source is not also reported as built or reused.
 */
export function tallySources(
   entries: Record<string, ManifestEntry>,
   failures: Record<string, SourceFailure>,
   carried: Record<string, ManifestEntry>,
): { sourcesBuilt: number; sourcesFailed: number; sourcesReused: number } {
   const built = Object.values(entries).filter(
      (e) => !isLegacyFailedEntry(e) && !carried[e.sourceEntityId!],
   );
   return {
      sourcesBuilt: built.length,
      sourcesFailed: Object.keys(failures).length,
      sourcesReused: Object.keys(carried).filter((id) => !failures[id]).length,
   };
}

/**
 * The record a run keeps for a source the eligibility gate refused, in the
 * plan's `RefusedSource` shape so both report it identically.
 */
export function refusalRecord(
   persistSource: PersistSource,
   err: MaterializationEligibilityError,
   tier: RefusedSource["tier"],
   modelPath: string | undefined,
): RefusedSource {
   return {
      name: persistSource.name,
      sourceID: persistSource.sourceID,
      ...(modelPath ? { modelPath } : {}),
      tier,
      reason: err.reason || "authorize",
      message: errMessage(err),
   };
}

/**
 * The run-level refusal when every targeted source was refused. Several
 * refusals are joined so the run names every source, not only the first.
 */
export function allSourcesRefusedError(
   errors: MaterializationEligibilityError[],
): MaterializationEligibilityError {
   if (errors.length === 1) return errors[0];
   const reason = errors.every((e) => e.reason === errors[0].reason)
      ? errors[0].reason
      : undefined;
   return new MaterializationEligibilityError({
      message: errors.map((e) => e.message).join("; "),
      reason,
   });
}

/**
 * Redact the concrete credential values found in the given connections out of
 * an error message, keeping the rest legible ("schema not found" still tells
 * the operator what to fix) while never leaking passwords, secrets, service
 * account JSON or connection strings a federation or attach error can echo.
 */
export function redactConnectionSecrets(
   message: string,
   ...connections: unknown[]
): string {
   const secrets = new Set<string>();
   // One `seen` across arguments: a source and destination may share a subgraph.
   const seen = new WeakSet<object>();
   for (const c of connections) collectSensitiveValues(c, secrets, seen);
   let redacted = redactPgSecrets(message);
   for (const s of secrets) {
      redacted = redacted.split(s).join("***");
      // A DuckDB error may echo the SQL, where a secret with a single quote
      // appears escaped (`''`), so redact the escaped form too.
      const escaped = escapeSQL(s);
      if (escaped !== s) redacted = redacted.split(escaped).join("***");
   }
   return redacted;
}

/** Classify a thrown build error as cancelled (cooperative abort) or failed. */
export function outcomeFor(
   _err: unknown,
   signal: AbortSignal | undefined,
): "failed" | "cancelled" {
   return signal?.aborted ? "cancelled" : "failed";
}

/**
 * Allowed status transitions: PENDING → MANIFEST_ROWS_READY (tables built) →
 * MANIFEST_FILE_READY. MANIFEST_FILE_READY, FAILED and CANCELLED are terminal.
 */
export const VALID_TRANSITIONS: Record<
   MaterializationStatus,
   MaterializationStatus[]
> = {
   PENDING: ["MANIFEST_ROWS_READY", "FAILED", "CANCELLED"],
   MANIFEST_ROWS_READY: ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"],
   MANIFEST_FILE_READY: [],
   FAILED: [],
   CANCELLED: [],
};

/**
 * The key a ledger entry and the instruction that builds its table must agree
 * on. The connection and destination are separate namespaces that may share a
 * name, so the parts are NUL-delimited: connection `lake` and destination
 * `lake` must not produce one key.
 */
export function tableKeyOf(
   connectionName: string,
   storageDestinationName: string | undefined,
   physicalTableName: string,
): string {
   return [
      connectionName,
      storageDestinationName ?? "",
      physicalTableName,
   ].join("\u0000");
}

/**
 * Name the stored tables a failed run wrote and did not reclaim. Otherwise
 * there is no record at all (no manifest committed, and the host's orphan
 * sweep does not cover storage destinations; see
 * {@link isReclaimableStorageTable}).
 *
 * Deliberately does NOT call them orphans: `refresh: "full"` covers both a
 * rebuild at a fresh generational name (unreferenced) and a seed fallback on
 * the stable name a previous manifest still binds (live). It reports only what
 * is certain.
 */
export function reportRetainedStorageTables(
   retained: ManifestEntry[],
   packageName: string,
): void {
   for (const entry of retained) {
      recordStorageTableRetained();
      logger.warn(
         "A failed run left a table in a storage destination and did not reclaim " +
            "it: the source is refreshed incrementally, so this name may be the " +
            "one it serves from. Unreferenced if the run was building a fresh " +
            "generation, in which case reclaiming it needs the destination sweep.",
         {
            packageName,
            sourceName: entry.sourceName,
            destinationName: entry.storageDestinationName,
            physicalTableName: entry.physicalTableName,
            refresh: entry.refresh,
         },
      );
   }
}

/**
 * Whether a manifest entry names a stored table a failed run may reclaim.
 *
 * An INCREMENTALLY REFRESHED source (`entry.refresh` present) is never
 * reclaimed: its physical name is stable across runs, a prior manifest may bind
 * it, and a refresh writes it in place. Dropping it because a later source
 * failed would take the source off its table until a rebuild. A source with a
 * stored upstream is retained on the same terms, because the host sees its
 * `refresh="incremental"` and hands it a stable serving name too.
 *
 * Conservative in one direction: an incremental source's FIRST (or forced)
 * build is at a fresh, unbound name that `entry.refresh` cannot distinguish,
 * so it leaks. The host's orphan sweep does not catch it (it enumerates a
 * connection's catalog and skips storage-destination rows), and the reclaim's
 * `stillReferenced` check only sees READY manifests of the SAME package name.
 * Accepted: a destination already accumulates superseded generations, and
 * leaking costs storage while reclaiming a serving table costs data. The
 * premise (an incremental source is built at the name it serves from) is the
 * host's to keep; minting generations per source rather than per address
 * breaks it.
 */
export function isReclaimableStorageTable(entry: ManifestEntry): boolean {
   return !!entry.storageDestinationName && entry.refresh === undefined;
}

/**
 * The physical table an instruction writes, as a comparable key: which table it
 * IS, where `sourceEntityId` says what it CONTAINS. JSON-encoded because a
 * physical name can contain a space. Exact-match, so `Foo`, `foo` and `"foo"`
 * are distinct here though one table on a case-folding engine (the same
 * blind spot as Package.persistenceCollisionWarnings).
 */
export function physicalTargetKey(
   instruction: BuildInstruction,
   connectionName: string,
): string {
   return JSON.stringify(
      instruction.destination
         ? [
              "destination",
              instruction.destination,
              instruction.physicalTableName,
           ]
         : ["connection", connectionName, instruction.physicalTableName],
   );
}
