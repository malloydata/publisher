// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { PersistSource } from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import { MaterializationEligibilityError } from "../errors";
import { logger } from "../logger";
import { malloyGivenToApi, type MalloyGiven } from "./given";
import { recordStorageTableRetained } from "../materialization_metrics";
import {
   BuildInstruction,
   isLegacyFailedEntry,
   LedgerEntry,
   MaterializationStatus,
   ManifestEntry,
   RefusedSource,
   SourceFailure,
} from "../storage/DatabaseInterface";
import { errMessage } from "../utils";
import {
   computeSourceEntityId,
   deriveAnnotationFields,
   compilerBuildId,
} from "./build_plan";
import {
   type IncrementalLineage,
   type WatermarkBound,
} from "./incremental_apply";
import { type QueryContext, type QueryMetadata } from "./query_metadata";
import type { components } from "../api";
import { getPersistStorageMode } from "../config";
import { resolvePartitionColumns } from "./persist_partition";
import { escapeSQL } from "./connection";
import {
   groupAliasesByName,
   type ServeShapeGiven,
} from "./materialization_serve_transform";

import type { ApiConnection } from "./model";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { redactPgSecrets } from "../pg_helpers";

/**
 * What a build needs to know from the plan about stored tables, to tell which
 * of the sources a downstream reaches are stored and whether their tables are
 * at hand. Both are the PLAN's facts, not a definition's: `#@ persist` is
 * inherited and `extend` never changes the SQL, so a plain extension or a rename
 * of a persisted source is a persist source too and shares the base's table,
 * and nothing on a definition says which name wrote the annotation (the
 * compiler copies the base's notes onto an extension). A stored table is a
 * content address, and every name whose SQL hashes to it is that table —
 * which is what `aliasesBySourceName` records.
 */
export interface ChainedPlanFacts {
   /**
    * Every persist source's `sourceID`, inheritors and renames included. By id,
    * not name: a name is not unique across a package's models (two files may
    * each declare `daily`), and the walk has each definition's id.
    */
   persistSourceIds: ReadonlySet<string>;
   /** The names sharing one table, keyed by each of them (groupAliasesByName). */
   aliasesBySourceName: Record<string, string[]>;
   /**
    * Each source's content address by `sourceID` — the key the publisher files
    * its manifest entry under. Absent for a source whose SQL could not be
    * rendered.
    */
   addressBySourceId: Record<string, string | undefined>;
   /**
    * Each source's key as the compiler computes it (`compilerBuildId`): the
    * address without the `partition=` layout the publisher folds in. Equal to
    * the address for an unpartitioned source; different for a partitioned one,
    * whose entry the compiler can then find only under this key.
    */
   compilerKeyBySourceId: Record<string, string | undefined>;
}

/** A build with no plan to consult: nothing is known to be stored. */
export const NO_PLAN_FACTS: ChainedPlanFacts = {
   persistSourceIds: new Set(),
   aliasesBySourceName: {},
   addressBySourceId: {},
   compilerKeyBySourceId: {},
};

/**
 * The run's manifest as the compiler must see it: every entry under the
 * publisher's address, and a partitioned source's entry under the compiler's
 * own key as well, since `persistedTableFor` looks a table up by that key and
 * would otherwise inline — or, strictly, miss — a stored partitioned upstream.
 * Only for the compare and strict renders over the full manifest; the ledger
 * and the manifest entries themselves stay keyed by address.
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
 * same lookup the compiler made: `getSQL` substitutes a manifest entry for a
 * persist source when the manifest holds the source's content address, and
 * inlines the source's definition otherwise. So an upstream was read from its
 * table exactly when its address is in `substituted` — the manifest this
 * build's SQL was rendered with — whatever the entry was called or where it
 * came from (an entry handed over by reference carries no `sourceName`, and a
 * lookup by name would call that a recompute).
 *
 * `inManifestOnly` are upstreams the full manifest holds but `substituted` does
 * not: entries a `storage=` destination holds, which `manifestExcludingStorage`
 * drops because a warehouse build cannot read them. For a `storage=` build they
 * are the parents to stack on; for a colocated build they are recomputed.
 * `missing` are upstreams the build SQL reads that no manifest holds — neither
 * built in this run nor supplied by reference — which only a recompute can
 * supply. They come from the compiler, not the walk (`compilerMissing`, see
 * {@link compilerMissingUpstream}): the walk follows every declared join and
 * stops at a persist source's own definition, so it both over-reaches (a join
 * the query never reads) and under-reaches (a join declared on the stop
 * itself). What the SQL reads is the compiler's to say, and a walk stop that
 * no manifest holds and the compiler did not miss is one the SQL does not
 * read — not a recompute, and not a reason. A stop whose address is the
 * root's own (`rootAddress`) is the table being built, not an upstream: a
 * persisted extension shares its base's address, and building it runs the
 * defining query. An upstream with no address (its SQL could not be rendered)
 * is reported as recomputed and never refused on that account — a build that
 * truly cannot render it fails on its own.
 *
 * `sqlInlinesStored` is the compiler's own reach, as a floor under the walk.
 * The walk stops at a persist source and follows only an extension's added
 * joins, so a join declared on the stored stop itself — `daily` carrying
 * `join_one: c is daily_counts`, with `daily_counts` in a `storage=` destination
 * — is reached by the compiler (it inlines `daily_counts` into the build SQL)
 * and not by the walk. Rendering the source's SQL with every stored entry
 * available and comparing it with the SQL this build runs says whether some
 * stored table was inlined, whatever the walk named; when it was, the answer
 * is `recomputed` even if every upstream the walk names was read from its
 * table. That is also what gates a `storage=` build's stack-on-parent attempt.
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
      // No address: its SQL could not be rendered, so if this build read it the
      // build's own render would have failed first. Reached but unread; not an
      // upstream to report on.
      if (address === undefined) continue;
      upstreams++;
      if (address in params.substituted) continue;
      if (address in params.full) {
         // Held by a storage destination and excluded from the warehouse
         // manifest. Read by this SQL only if the compare says a stored table
         // was inlined; otherwise it is a declared join the query never uses,
         // and neither a recompute nor a reason.
         if (!params.sqlInlinesStored) continue;
         inManifestOnly.push(name);
         if (params.tier === "colocated") {
            const destination =
               params.builtEntries[address]?.storageDestinationName ??
               "a storage destination";
            reasons.push(
               `persisted upstream '${name}' is materialized in storage ` +
                  `destination '${destination}', which a warehouse build cannot read`,
            );
         }
         continue;
      }
      // In no manifest and not missed by the compiler: a declared join the
      // query never reads. Not in the SQL, so neither recomputed nor a reason.
   }
   for (const { name } of params.compilerMissing) {
      missing.push(name);
      reasons.push(
         `persisted upstream '${name}' is not in this build's manifest ` +
            `(neither built in this run nor supplied by reference)`,
      );
   }
   // The compare says a stored table was inlined and no stop accounts for it:
   // a stored-tier parent the walk did name is what a storage build stacks on,
   // not a reason; one it did not name is a join on a stored stop's own
   // definition, which only the compiler reached.
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
 * A compiler strict manifest miss, recognized by its `code` — the contract —
 * with the `sourceID` it names read from the message when the message has the
 * shape core writes today. The code alone makes it a miss; a message core has
 * reworded yields a miss of an unnamed source rather than no miss at all, so
 * strict still refuses and non-strict still recomputes (with a generic
 * reason) instead of the build failing on an error nobody classified. The
 * real-compiler spec pins the current wording; the id belongs on the error
 * object, which is asked of core in malloydata/malloy#3133.
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
 * The persisted source this build's SQL reads that no manifest holds, as the
 * compiler sees it: the source rendered once more with the run's whole
 * manifest and `strict: true`, so the only miss possible is a persist source
 * neither built in this run nor supplied by reference that the SQL actually
 * reads. Declared joins the query never uses are not rendered and so not
 * missed; a join declared on a stored stop's own definition is. The compiler
 * stops at the first miss, so this names at most one source per build — the
 * refusal or the reason names it, and the next build names the next.
 */
export function compilerMissingUpstream(
   persistSource: PersistSource,
   fullManifest: Manifest["buildManifest"],
   connectionDigests: Record<string, string>,
   sourceNameById: ReadonlyMap<string, string>,
): { name: string; sourceID: string }[] {
   try {
      persistSource.getSQL({
         buildManifest: { ...fullManifest, strict: true },
         connectionDigests,
      });
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
 * This is the table→names map `Runtime.getBuildTargets()` returns as
 * `target.sources` (malloydata/malloy#3029), derived here from the deprecated
 * per-source plan with the publisher's own address recipe —
 * `computeSourceEntityId` is pinned equal to `target.buildId` by
 * `build_targets_address_equality.spec.ts`, so the groups are the same ones.
 * Not a call to `getBuildTargets` yet, for one reason: `mkBuildTargets` renders
 * every target's SQL while it plans, with no guard, so one persist source that
 * cannot render — a free parameter, a given, refused on its own path — fails the
 * call for the whole model, where this skips just that source. (The same eager
 * render is why the chained build's transient model stays on `getBuildPlan`:
 * its rebound parents are virtual sources, which cannot render without the
 * `virtualMap` that call cannot take. See `buildDownstreamIntoStorage`.)
 *
 * When the builder moves to `getBuildTargets`, this is the one place the
 * chained build reads plan facts from: per model, `runtime.getBuildTargets`
 * → every `target.sources` id into `persistSourceIds` and its `buildId` into
 * `addressBySourceId`, each target's names as one alias group. That move
 * likely wants a core change first — a planning mode that reports an
 * unrenderable source instead of throwing, and takes a `virtualMap` — or a
 * pre-filter of the sources the eligibility gate refused. It also retires
 * `compilerKeyBySourceId` and `manifestForCompiler`: `computeSourceEntityId`
 * folds a `partition=` layout into the address and the compiler's own table
 * lookup (`mkBuildID` of digest and SQL) does not, so the renders that hand
 * the compiler the full manifest carry a partitioned entry under both keys.
 * With `target.buildId` as the address there is one key.
 */
export function chainedPlanFacts(
   sources: Record<string, PersistSource>,
   connectionDigests: Record<string, string>,
): ChainedPlanFacts {
   const planSources: { name: string; sourceEntityId?: string }[] = [];
   const addressBySourceId: Record<string, string | undefined> = {};
   const compilerKeyBySourceId: Record<string, string | undefined> = {};
   for (const [sourceID, source] of Object.entries(sources)) {
      let sourceEntityId: string | undefined;
      try {
         sourceEntityId = computeSourceEntityId(source, connectionDigests);
         compilerKeyBySourceId[sourceID] = compilerBuildId(
            source,
            connectionDigests,
         );
      } catch {
         // A source whose SQL cannot be rendered (an unbound parameter, a
         // given) has no address to group by; the eligibility gate refuses it
         // on its own path, and here it is a name with no table-mates.
      }
      planSources.push({ name: source.name, sourceEntityId });
      addressBySourceId[sourceID] = sourceEntityId;
   }
   return {
      persistSourceIds: new Set(Object.keys(sources)),
      aliasesBySourceName: groupAliasesByName(planSources),
      addressBySourceId,
      compilerKeyBySourceId,
   };
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
 * What an incremental refresh DID to the table, as the manifest entry's field,
 * or nothing for a source that is not refreshed incrementally at all.
 *
 * Reported because it cannot be inferred: every fallback in the delta path
 * answers success and rebuilds, so a caller that instructed a delta and got a
 * rebuild sees an identical entry otherwise. It is also how a caller supplying
 * its own ledger tells that its entries are being advanced from.
 *
 * `none` names the do-nothing case rather than leaving it to absence, so absence
 * carries ONE meaning: this source is not refreshed incrementally. The
 * alternative reads the same for a skip and for a plain full-refresh source, and
 * `ledger` cannot break the tie — an incremental source over an empty table
 * reports no boundary either.
 */
export function refreshFields(
   did: "delta" | "full" | "none" | undefined,
): { refresh: "delta" | "full" | "none" } | Record<never, never> {
   return did ? { refresh: did } : {};
}

/**
 * The manifest entry's `ledger` field: the boundary this refresh left in force,
 * self-contained — the table it belongs to, the value, and the source
 * definition it was measured under. A caller that owns the ledger stores this
 * object and returns it verbatim in the next run's `buildInstructions.ledger`;
 * nothing else about it needs to be understood, which is the point.
 *
 * The ONLY place a boundary is reported, which is why it is emitted whoever owns
 * the ledger, and on every path that leaves one in force — a delta, a seed, or a
 * skip that applied nothing. It is also how progress is observed: a
 * `coveredThrough` that moved between runs is a delta that applied. The boundary
 * is not repeated on the entry itself, because the value alone is not
 * comparable across runs: it means something only alongside the watermark and
 * the source address it was measured under, which are right here.
 *
 * Nothing when there is no boundary: a non-incremental source, or an incremental
 * one whose source is empty (no boundary is derivable, so none is recorded).
 * Spread into the entry so that case adds no key at all rather than an explicit
 * undefined.
 */
export function ledgerFields(
   lineage: IncrementalLineage | undefined,
   bound: WatermarkBound | undefined,
): { ledger: LedgerEntry } | Record<never, never> {
   if (!lineage || !bound) return {};
   return {
      ledger: {
         connectionName: lineage.connectionName,
         physicalTableName: lineage.physicalTableName,
         // Absent for a colocated table, which is what absence MEANS on the wire.
         // Spread rather than set to undefined so the field is not present-and-null
         // for every colocated source a caller stores.
         ...(lineage.storageDestinationName
            ? { storageDestinationName: lineage.storageDestinationName }
            : {}),
         coveredThrough: bound.value,
         coveredThroughType: bound.malloyType,
         watermark: lineage.watermarkName,
         ...(lineage.mergeKeys.length > 0
            ? { mergeKeys: lineage.mergeKeys }
            : {}),
         strategy: lineage.strategy,
         sourceEntityId: lineage.sourceEntityId,
      },
   };
}

/**
 * The narrow environment surface the build path needs to materialize into a
 * `storage=` destination, and the environment root (to derive a plain-DuckDB
 * destination's file path). Kept minimal to avoid coupling the materialization
 * service to the full Environment type.
 *
 * The two lookups are separate on purpose, and which one a call site uses is the
 * whole security property: `getApiConnection` resolves the SOURCE the data is
 * read from, a warehouse the package's own model already names, while
 * `getStorageDestination` resolves where the table is WRITTEN. They read
 * disjoint lists, so a `storage=` build can never be steered into a connection
 * by naming it — including one whose name matches a destination.
 */
export interface BuildEnvironment {
   getApiConnection(connectionName: string): ApiConnection;
   getStorageDestination(destinationName: string): ApiConnection;
   getEnvironmentPath(): string;
}

/**
 * What a build needs to tag the statements it issues: the package-level layer of
 * per-query metadata, and the run's own context. Assembled once per run — every
 * field is constant for the run, so a source only adds its own name.
 */
export interface BuildQueryMetadata {
   packageMaterialization:
      | components["schemas"]["PackageMaterializationConfig"]
      | null;
   context: QueryContext;
}

/**
 * A connection's two metadata layers: the overridable default and the properties
 * the deployment enforces.
 *
 * Fails open — a build must not fail because a connection's config could not be
 * read for its tags — so an unreadable connection costs the layers, never the
 * statement.
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
      // Diagnosable rather than silent: the layer this costs is the enforced
      // one, and it is the only drop with no metric behind it.
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
   // Drop hyphens so the suffix stays a bare identifier fragment when the id
   // becomes a UUID5 (a no-op for the current hex ids).
   return `_${sourceEntityId.replace(/-/g, "").substring(0, STAGING_ID_LEN)}`;
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
 * The build manifest for a `storage=` build, with storage-materialized entries
 * removed. A storage build runs in the source warehouse (passthrough), so it
 * cannot reference an upstream that landed in a DuckDB/DuckLake store — dropping
 * those entries makes the compiler INLINE the upstream (non-strict) or raise a
 * clean strict-miss (strict), instead of emitting a cross-engine table
 * reference. colocated entries are kept — the warehouse build can
 * reference them, carrying forward the DIALECT-QUOTED table path the seed loop
 * already stamped (publisher #904's quoteSeedTablePath), so the downstream FROM
 * resolves a case-preserved upstream on a case-folding engine. Preserves the
 * manifest's `strict` flag.
 */
export function manifestExcludingStorage(
   manifest: Manifest,
   builtEntries: Record<string, ManifestEntry>,
): Manifest["buildManifest"] {
   const reduced = new Manifest();
   reduced.strict = manifest.strict;
   // The source manifest's entries are already dialect-quoted (#904); reuse that
   // quoting rather than the raw physical name so the kept colocated references
   // stay canonical for the downstream FROM.
   const quoted = manifest.buildManifest.entries;
   for (const [id, entry] of Object.entries(builtEntries)) {
      if (!entry.storageDestinationName && quoted[id]) {
         reduced.update(id, { tableName: quoted[id].tableName });
      }
   }
   return reduced.buildManifest;
}

/**
 * The `storage=` destination a source DECLARES (external-tier intent), or
 * undefined. Independent of `PERSIST_STORAGE_MODE` — reflects author intent, so
 * the build can tell a `storage=` source apart from a plain colocated
 * `#@ persist` even when the tier is off.
 */
export function declaredStorage(
   persistSource: PersistSource,
): string | undefined {
   return deriveAnnotationFields(persistSource).storage?.trim() || undefined;
}

/**
 * The columns this source's table is laid out by, for a build that is about to
 * run.
 *
 * Re-resolved here rather than carried from the eligibility gate because the
 * gate and the build are far apart in this flow, and a value threaded through
 * that distance is one that can go stale. The resolution is pure over the source
 * and its annotation, so the two cannot disagree — but that is a contract, not a
 * coincidence, so a refusal reaching here THROWS. The gate has already refused
 * every shape this can refuse; arriving at a build means it did not, and
 * quietly building unpartitioned would turn a broken `partition=` into a table
 * whose layout silently differs from what the author declared.
 */
/**
 * The author model's given surface, for the chained build's transient model.
 * `Model.givens` has collapsed inheritance through imports, so this is the
 * surface the author's own files compile against. A given whose type cannot
 * be rendered is dropped rather than guessed: text that reads it then fails
 * to compile and the build reports a shape it could not carry, where a
 * guessed type would compile a predicate that silently coerces.
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
 * Resolve a persist source's `#@ persist storage=<ref>` to the EFFECTIVE
 * destination connection name for a build, or undefined for the default
 * colocated path (the source materializes into its own warehouse). Read
 * publisher-side from the compiled annotation (the same `annotationFields` map
 * the plan echoes); the reference resolves generically against registered
 * connections. Absent `storage=` ⇒ undefined (colocated); any value names a
 * registered connection to materialize into. Any managed-tier alias is resolved
 * by the host upstream and set on the wire instruction's `destination` — it
 * never reaches this publisher-side generic resolution.
 *
 * When `PERSIST_STORAGE_MODE=off` this returns undefined regardless of the
 * annotation, so the feature is a runtime kill switch that never fails a
 * package (the ignored `storage=` is surfaced as a package warning, not an
 * error). Undefined here does NOT mean "build it colocated" for a source that
 * declared `storage=` — `deriveSelfInstructions` skips such a source entirely so
 * it serves live; see {@link declaredStorage}.
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
   // The callers pass a LIVE connection, not its config, so this walks driver
   // and pool internals whose graph can contain a back-reference. Without this
   // guard such a cycle recurses until the stack is exhausted, and the
   // RangeError replaces the build error being redacted -- turning any failure
   // on a cyclic connection into "Maximum call stack size exceeded". Mirrors the
   // guard in redactSensitive.
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
 * Redact the actual credential values (from the given connection configs) out
 * of an error message, then surface the message. This keeps a build error
 * legible — a "schema not found" or "table does not exist" tells the operator
 * exactly what to fix — while never leaking the passwords / secrets / service
 * account JSON / connection strings a federation or attach error can echo. Only
 * the concrete secret values are removed, not the message structure.
 */
/**
 * How many sources a run built, failed on, and reused, counted from what the
 * build returned rather than from what it was asked to do.
 *
 * The instruction list is the wrong denominator because an instruction can be
 * skipped without building (no matching compiled source), so counting
 * instructions would report a skip as built.
 *
 * `failures` is the authority on what failed, and an entry mirroring a failure
 * (the `ManifestEntry.error` deprecation window) is excluded from every other
 * count -- otherwise one lost source reports as built, or as both reused and
 * failed where it was also seeded.
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
 * build plan's `RefusedSource` shape so the plan and the run report one refusal
 * identically.
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
 * The run-level refusal for a run whose every targeted source was refused, so
 * there is nothing to build. One refusal is rethrown as it was raised; several
 * are joined so the run names every source it could not build, not only the
 * first in plan order.
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

export function redactConnectionSecrets(
   message: string,
   ...connections: unknown[]
): string {
   const secrets = new Set<string>();
   // One `seen` across every argument: the multi-connection sites hand in a
   // source and a destination that share a subgraph, and walking it twice
   // collects nothing new.
   const seen = new WeakSet<object>();
   for (const c of connections) collectSensitiveValues(c, secrets, seen);
   let redacted = redactPgSecrets(message);
   for (const s of secrets) {
      redacted = redacted.split(s).join("***");
      // A DuckDB error often echoes the offending SQL statement, in which a
      // secret containing a single quote appears single-quote-escaped (`''`) —
      // so the raw value won't match. Also redact the escaped form.
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
 * Allowed status transitions. The build runs without an intermediate
 * plan-ready pause: PENDING advances straight to MANIFEST_ROWS_READY once the
 * tables are built, then to MANIFEST_FILE_READY. MANIFEST_FILE_READY, FAILED,
 * and CANCELLED are terminal.
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
 * The key a ledger entry and the instruction that builds its table must agree on.
 *
 * Three parts, because where a table LIVES is not implied by the connection whose
 * SQL computes it: a stored table is read from its warehouse and written to a
 * destination, and those are separate namespaces that may share a name. The parts
 * are NUL-delimited rather than concatenated for that reason — a connection named
 * `lake` and a destination named `lake` must not produce one key.
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
 * Name the stored tables a failed run wrote and did not reclaim.
 *
 * Without this there is no record at all: the run committed no manifest, the
 * reclaim was never handed these entries, and the only trace a table was written
 * is its absence from everything. That matters more here than it would elsewhere,
 * because the host's orphan sweep does not cover a storage destination (see
 * {@link isReclaimableStorageTable}), so nothing downstream will notice either.
 *
 * Deliberately does NOT claim these are orphans. Two different outcomes report
 * `refresh: "full"` and the entry cannot separate them: a host-initiated rebuild
 * at a FRESH generational name, which nothing references and nothing will build
 * again, and a publisher-side seed fallback on the stable name a previous manifest
 * still binds, which is live and correct. So it reports what is certain — this run
 * wrote here and left it — and leaves the classification to whoever reads it.
 */
export function reportRetainedStorageTables(
   retained: ManifestEntry[],
   packageName: string,
): void {
   for (const entry of retained) {
      recordStorageTableRetained(entry.storageDestinationName ?? "unknown");
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
 * An INCREMENTALLY REFRESHED source is never reclaimed, whatever this run did to
 * its table. Its physical name has to be stable across runs or its boundary never
 * matches, so that name is not a fresh generation nobody else knows about — a
 * prior manifest may bind it, and a refresh writes it in place either way: a delta
 * as DML, a re-seed as `CREATE OR REPLACE`. Dropping it because a LATER source in
 * the run failed takes the source off its stored table until some rebuild puts it
 * back, and it is the same harm whether the run advanced the table or rebuilt it
 * correctly. `entry.refresh` is present exactly for such a source, so its presence
 * is the test.
 *
 * A source whose upstream is itself stored is retained on the same terms, even though
 * it can never be advanced by a delta and so has no boundary of its own to protect.
 * Which name it is built at is the HOST's choice, and the host cannot see that this
 * source is chained: what it reads is `refresh="incremental"`, which this source
 * carries. So it is handed a stable serving name like any other incremental source,
 * and reclaiming that name would take the source off its table for the reason above.
 *
 * That makes this test conservative rather than exact, in one direction. An
 * incremental source's FIRST build, or one an operator forced, is written at a fresh
 * name that no manifest binds, and `entry.refresh` cannot tell that from a refresh in
 * place — so such a table is retained and leaks. What does separate them is the
 * per-instruction reseed a host sets for any build it gave a fresh name; that is a
 * host convention this code cannot check, which is why the entry alone does not
 * decide it.
 *
 * The premise the retain rests on — that a source declaring an incremental refresh is
 * built at the name it is already served from — is likewise the host's to keep. A host
 * that mints a generation per SOURCE rather than per content address breaks it wherever
 * two sources compile to one address, and a retained table may then be one that was
 * never serving.
 *
 * The cost is a leaked table, and it is worth being exact about who does NOT clean
 * it up. A build at a FRESH name whose run then failed is recorded by no manifest,
 * so this reclaim was the only thing that could have dropped it — and the host's
 * orphan sweep does not cover the gap: it enumerates a CONNECTION's catalog and
 * skips any row that names a storage destination, because a physical name carries
 * no destination and generation counting is per destination, so one string can name
 * a table in either place. Reclaiming a destination is tracked separately.
 *
 * Accepted anyway, because a stored destination already accumulates superseded
 * generations for that same reason, so this adds a case to a known leak rather
 * than a new class of one — and the alternative is dropping a table that is
 * serving. Not reclaiming costs storage; reclaiming costs a source its data until
 * something rebuilds it.
 *
 * The reclaim's own `stillReferenced` check does not cover any of this. It spares
 * a table that some READY manifest of the SAME package name serves, and a host
 * that version-qualifies its package names sees none of its own earlier manifests.
 */
export function isReclaimableStorageTable(entry: ManifestEntry): boolean {
   return !!entry.storageDestinationName && entry.refresh === undefined;
}

/**
 * The physical table an instruction writes, as a comparable key.
 *
 * The counterpart to `sourceEntityId`: the address says what a table CONTAINS,
 * this says which table it IS, and only this answers "have I written that
 * already". Encoded rather than concatenated because a physical name can contain
 * a space (see the `quoted-persist-name` scenarios), so a plain separator could
 * make two different coordinates read as one.
 *
 * Exact-match, and so blind in the same place its load-time sibling is: `Foo`,
 * `foo` and `"foo"` land in different slots here but in ONE table on a
 * case-folding engine. See the note on Package.persistenceCollisionWarnings —
 * neither guard is stricter than the other.
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
