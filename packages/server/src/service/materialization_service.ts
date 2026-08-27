// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type {
   Connection as MalloyConnection,
   PersistSource,
} from "@malloydata/malloy";
import { Manifest } from "@malloydata/malloy";
import {
   BadRequestError,
   InvalidStateTransitionError,
   MaterializationConflictError,
   MaterializationEligibilityError,
   MaterializationNotFoundError,
} from "../errors";
import { logger } from "../logger";
import {
   MaterializationMode,
   recordAutoLoadOutcome,
   recordChainedStorageBuild,
   recordDropTables,
   recordManifestBindDegraded,
   recordMaterializationRun,
   recordSourceBuildDuration,
   recordStorageTableRetained,
   recordSourcesOutcome,
   recordStorageBuildFailure,
} from "../materialization_metrics";
import {
   BuildInstruction,
   BuildManifestResult,
   BuildPlan,
   FreshnessManifest,
   isLegacyFailedEntry,
   LedgerEntry,
   Materialization,
   MaterializationStatus,
   MaterializationUpdate,
   ManifestEntry,
   ManifestReference,
   ResourceRepository,
   SourceFailure,
} from "../storage/DatabaseInterface";
import { DuplicateActiveMaterializationError } from "../storage/duckdb/MaterializationRepository";
import { errMessage } from "../utils";
import {
   collectIncrementalDeclarations,
   CompiledBuildPlan,
   compilePackageBuildPlan,
   computeSourceEntityId,
   deriveAnnotationFields,
   deriveColumns,
   projectToPublicColumns,
   iterGraphSources,
   resolveQueryMetadata,
} from "./build_plan";
import {
   warehouseDeltaTarget,
   type DeltaTarget,
   type IncrementalLineage,
   type WatermarkBound,
} from "./incremental_apply";
import {
   advanceLedgerAfterSeed,
   applyIncrementalStep,
   incrementalLineage,
   indexCallerLedger,
   planSourceRefresh,
   reportIncrementalStep,
   resetLedger,
   type IncrementalRunContext,
} from "./incremental_build";
import type { IncrementalDeclaration } from "./incremental_declaration";
import {
   mergeQueryMetadata,
   type QueryContext,
   type QueryMetadata,
} from "./query_metadata";
import type { components } from "../api";
import { getPersistStorageMode } from "../config";
import { EnvironmentStore } from "./environment_store";
import {
   assertColocatedPersistNotAuthorizeGated,
   assertMaterializationEligible,
} from "./materialization_eligibility";
import {
   assertStorageServeShapeCompiles,
   BilledReadNotCapturedError,
   buildDownstreamIntoStorage,
   buildSourceIntoStorage,
   dropStorageTable,
   STORAGE_TARGET_DIALECT,
   type StorageBuildResult,
   type StorageIncrementalRefresh,
} from "./materialization_build_session";
import { storageDeltaTarget } from "./incremental_storage";
import { escapeSQL } from "./connection";
import {
   buildChainedStorageBuildModel,
   buildVirtualMap,
   deriveServeBindings,
   type ServeBinding,
   type SourceLocation,
   sliceSourceRange,
} from "./materialization_serve_transform";
import type { ApiConnection } from "./model";
import { fetchManifestEntries, splitManifestEntries } from "./manifest_loader";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import {
   quoteRenameTarget,
   quoteManifestTablePath,
   quoteTablePath,
} from "./quoting";
import { resolveEnvironmentId } from "./resolve_environment";
import { redactPgSecrets } from "../pg_helpers";

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
function refreshFields(
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
function ledgerFields(
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
interface BuildEnvironment {
   getApiConnection(connectionName: string): ApiConnection;
   getStorageDestination(destinationName: string): ApiConnection;
   getEnvironmentPath(): string;
}

/**
 * What a build needs to tag the statements it issues: the package-level layer of
 * per-query metadata, and the run's own context. Assembled once per run — every
 * field is constant for the run, so a source only adds its own name.
 */
interface BuildQueryMetadata {
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
function connectionMetadataLayers(
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
const STAGING_ID_LEN = 12;

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
function selfAssignTableName(persistSource: PersistSource): string {
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
function declaredStorage(persistSource: PersistSource): string | undefined {
   return deriveAnnotationFields(persistSource).storage?.trim() || undefined;
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
function resolveStorageDestination(
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
function outcomeFor(
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
const VALID_TRANSITIONS: Record<
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
function tableKeyOf(
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
function reportRetainedStorageTables(
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
 * Orchestrates single-call materialization builds.
 *
 * The build plan is a deterministic property of the compiled package
 * (`Package.buildPlan`), so there is no separate plan round-trip. On create
 * the publisher either auto-runs (self-assigns physical names from the
 * `#@ persist name=` annotation and builds + auto-loads every persist source)
 * or, when the caller supplies `buildInstructions` derived from
 * `Package.buildPlan`, builds directly into the caller-assigned names without
 * auto-loading the manifest. Both paths build in the background and return the
 * PENDING record immediately.
 *
 * At most one active materialization per (environment, package) is enforced
 * by the DB-level unique index on `materializations.active_key` (see
 * {@link MaterializationRepository}). Cancellation is cooperative via
 * AbortController.
 */
export class MaterializationService {
   /** In-flight runs, so they can be cancelled. In-process only. */
   private runningAbortControllers = new Map<string, AbortController>();

   constructor(private environmentStore: EnvironmentStore) {}

   private get repository(): ResourceRepository {
      return this.environmentStore.storageManager.getRepository();
   }

   // ==================== STATE MACHINE ====================

   private validateTransition(
      current: MaterializationStatus,
      next: MaterializationStatus,
   ): void {
      if (!VALID_TRANSITIONS[current].includes(next)) {
         throw new InvalidStateTransitionError(
            `Cannot transition from ${current} to ${next}`,
         );
      }
   }

   private async transition(
      id: string,
      next: MaterializationStatus,
      extra?: Omit<MaterializationUpdate, "status">,
   ): Promise<Materialization> {
      const current = await this.repository.getMaterializationById(id);
      if (!current) {
         throw new MaterializationNotFoundError(
            `Materialization ${id} not found`,
         );
      }
      this.validateTransition(current.status, next);
      // Terminal transitions are operationally interesting (info); the
      // intermediate MANIFEST_ROWS_READY hop is routine bookkeeping (debug).
      const terminal =
         next === "MANIFEST_FILE_READY" ||
         next === "FAILED" ||
         next === "CANCELLED";
      logger[terminal ? "info" : "debug"]("Materialization transition", {
         materializationId: id,
         packageName: current.packageName,
         from: current.status,
         to: next,
      });
      return this.repository.updateMaterialization(id, {
         status: next,
         ...extra,
      });
   }

   // ==================== QUERIES ====================

   async listMaterializations(
      environmentName: string,
      packageName: string,
      options?: { limit?: number; offset?: number },
   ): Promise<Materialization[]> {
      const environmentId = await this.resolveEnvironmentId(environmentName);
      return this.repository.listMaterializations(
         environmentId,
         packageName,
         options,
      );
   }

   /**
    * Every materialization across all packages in an environment, newest first.
    * Each record carries its `packageName`, so an env-scoped view can group or
    * label by package without a per-package fan-out.
    */
   async listEnvironmentMaterializations(
      environmentName: string,
      options?: { limit?: number; offset?: number },
   ): Promise<Materialization[]> {
      const environmentId = await this.resolveEnvironmentId(environmentName);
      return this.repository.listMaterializationsByEnvironment(
         environmentId,
         options,
      );
   }

   /**
    * `created_at` of the newest scheduler-fired materialization for a package,
    * or null if none. The standalone scheduler uses this on its first arm to
    * recover a fire missed during downtime (see MaterializationScheduler.arm).
    */
   async getLatestScheduledFireAt(
      environmentName: string,
      packageName: string,
   ): Promise<Date | null> {
      const environmentId = await this.resolveEnvironmentId(environmentName);
      return this.repository.getLatestScheduledFireAt(
         environmentId,
         packageName,
      );
   }

   async getMaterialization(
      environmentName: string,
      packageName: string,
      id: string,
   ): Promise<Materialization> {
      const environmentId = await this.resolveEnvironmentId(environmentName);
      const m = await this.repository.getMaterializationById(id);
      if (
         !m ||
         m.environmentId !== environmentId ||
         m.packageName !== packageName
      ) {
         throw new MaterializationNotFoundError(
            `Materialization ${id} not found for package ${packageName}`,
         );
      }
      return m;
   }

   // ==================== CREATE + BUILD ====================

   /**
    * Create a materialization and build it in the background. Returns the
    * PENDING record immediately.
    *
    * Auto-run (default, no `buildInstructions`): self-assign physical names and
    * build + auto-load every persist source. Orchestrated (`buildInstructions`
    * present): build directly into the caller-assigned names from the package's
    * build plan, without auto-loading the manifest. When `buildInstructions` is
    * present it is validated synchronously against the package's compiled build
    * plan so a bad instruction is rejected at create time rather than failing
    * the background run.
    */
   async createMaterialization(
      environmentName: string,
      packageName: string,
      options: {
         forceRefresh?: boolean;
         /**
          * Rebuild the in-scope incremental sources from scratch, ignoring their
          * recorded boundaries. Distinct from `forceRefresh`, which only defeats
          * skip-if-unchanged and never re-seeds.
          */
         reseed?: boolean;
         sourceNames?: string[];
         buildInstructions?: BuildInstruction[];
         referenceManifest?: ManifestReference[];
         strictUpstreams?: boolean;
         /**
          * `BuildInstructions.ledger`: the incremental ledger for this run,
          * supplied by the caller (each entry a `ManifestEntry.ledger` an
          * earlier run reported). When present — even empty — it replaces the
          * publisher's local store for the whole run. Orchestrated-only, and
          * validated against the build plan at create time so an entry that
          * names an uninstructed table or a moved source is a 400, not a
          * failed background run.
          */
         ledger?: LedgerEntry[];
         /**
          * What initiated this run. `ON_DEMAND` (default) = a manual/API create;
          * `SCHEDULER` = the standalone materialization scheduler firing a
          * package's `materialization.schedule` cron. Recorded on the run
          * metadata so a scheduled rebuild is distinguishable from a manual one.
          */
         trigger?: "ON_DEMAND" | "SCHEDULER";
         /**
          * What the caller knows about this run and the publisher does not,
          * attached as query metadata to the statements the build issues. Its
          * `trigger` also covers the case the publisher's own `trigger` cannot
          * express (a publish), and its `runId` lets a caller's own id group the
          * build's statements instead of the publisher's materialization id.
          */
         runContext?: components["schemas"]["RunContext"] | null;
      } = {},
   ): Promise<Materialization> {
      const environmentId = await this.resolveEnvironmentId(environmentName);

      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const pkg = await environment.getPackage(packageName, false);

      const buildInstructions = options.buildInstructions;
      const orchestrated = buildInstructions !== undefined;
      if (orchestrated) {
         this.validateInstructions(
            pkg.getBuildPlan(),
            buildInstructions,
            options.ledger,
            options.reseed ?? false,
         );
      }

      const active = await this.repository.getActiveMaterialization(
         environmentId,
         packageName,
      );
      if (active) {
         throw this.activeConflict(packageName, active.id);
      }

      const forceRefresh = options.forceRefresh ?? false;
      const reseed = options.reseed ?? false;
      const trigger = options.trigger ?? "ON_DEMAND";
      const metadata = {
         forceRefresh,
         // Recorded separately from forceRefresh because it answers a different
         // question about the run — whether an incremental source rebuilt or
         // advanced — and the history is where that is read back.
         reseed,
         sourceNames: options.sourceNames ?? null,
         mode: orchestrated ? "orchestrated" : "auto",
         trigger,
      };

      let created: Materialization;
      try {
         created = await this.repository.createMaterialization(
            environmentId,
            packageName,
            "PENDING",
            metadata,
         );
      } catch (err) {
         if (err instanceof DuplicateActiveMaterializationError) {
            const winner = await this.repository.getActiveMaterialization(
               environmentId,
               packageName,
            );
            throw this.activeConflict(packageName, winner?.id);
         }
         throw err;
      }

      this.runInBackground(created.id, (signal) =>
         this.runBuild(
            created.id,
            environmentName,
            packageName,
            {
               sourceNames: options.sourceNames,
               forceRefresh,
               reseed,
               buildInstructions,
               // Orchestrated-only, and clamped here rather than trusted: an
               // auto-run has no caller to take a ledger from, and taking one
               // would replace the local store it depends on.
               ledger: orchestrated ? options.ledger : undefined,
               referenceManifest: options.referenceManifest,
               strictUpstreams: options.strictUpstreams,
               trigger,
               runContext: options.runContext ?? undefined,
            },
            signal,
         ),
      );

      return created;
   }

   /**
    * Single-call build, shared by auto-run and orchestrated mode. Compiles the
    * package build plan, derives the build instructions (self-assigned for
    * auto-run; caller-supplied for orchestrated), builds the instructed sources
    * into their physical tables, and commits the manifest. Auto-run additionally
    * loads the fresh manifest into the package models; orchestrated leaves
    * distribution to the caller.
    */
   private async runBuild(
      id: string,
      environmentName: string,
      packageName: string,
      opts: {
         sourceNames: string[] | undefined;
         forceRefresh: boolean;
         reseed: boolean;
         buildInstructions: BuildInstruction[] | undefined;
         ledger: LedgerEntry[] | undefined;
         referenceManifest: ManifestReference[] | undefined;
         strictUpstreams: boolean | undefined;
         trigger: "ON_DEMAND" | "SCHEDULER";
         runContext?: components["schemas"]["RunContext"];
      },
      signal: AbortSignal,
   ): Promise<void> {
      const orchestrated = opts.buildInstructions !== undefined;
      const mode: MaterializationMode = orchestrated ? "orchestrated" : "auto";
      logger.info("Materialization build started", {
         materializationId: id,
         packageName,
         mode,
      });
      const startedAt = Date.now();

      try {
         // Persist the run's start time so the UI can compute a duration
         // (start -> now while in-flight, start -> completed once terminal).
         await this.repository.updateMaterialization(id, {
            startedAt: new Date(startedAt),
         });

         const environmentId = await this.resolveEnvironmentId(environmentName);
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const pkg = await environment.getPackage(packageName, false);

         const compiled = await environment.withPackageLock(packageName, () =>
            compilePackageBuildPlan(pkg, signal),
         );

         // Backstop: refuse loudly if the package annotated a `#@ persist` source
         // that Malloy's getBuildPlan() silently dropped (a shape it doesn't
         // treat as a materializable root — see detectDroppedPersistSources).
         // Without this the build would report success with an empty manifest and
         // the source would serve live, contradicting the "hard refuse, never a
         // silent fallback" contract. Scoped to the sources this build targets so
         // a build of unrelated sources isn't blocked by a dropped sibling.
         const relevantDropped = (compiled.droppedPersistSources ?? []).filter(
            (d) => !opts.sourceNames || opts.sourceNames.includes(d.name),
         );
         if (relevantDropped.length > 0) {
            const names = relevantDropped.map((d) => `'${d.name}'`).join(", ");
            throw new MaterializationEligibilityError({
               message:
                  `Source(s) ${names} are annotated '#@ persist' but were not ` +
                  `recognized as a materializable source, so nothing would be ` +
                  `built (they would be served live). Only query/aggregate ` +
                  `sources materialize; a filtered pass-through does not. Persist ` +
                  `a query source, or invoke a parameterized source with a bound ` +
                  `argument, or drop the annotation to serve live.`,
            });
         }

         // Assembled BEFORE the instructions because skip-if-unchanged consults
         // it: an incremental source is exempt from the carry-forward below.
         const incremental = this.incrementalRunContext(compiled, {
            environmentId,
            packageName,
            materializationId: id,
            // `reseed`, never `forceRefresh`. The latter only defeats
            // skip-if-unchanged, which an incremental source is exempt from
            // anyway, so it has nothing to say about how one is built — and every
            // scheduled fire sets it, which would make a schedule a series of
            // full rebuilds that no delta could ever follow.
            forceRefresh: opts.reseed ?? false,
            now: new Date(startedAt),
            ledger: opts.ledger,
         });

         let instructions: BuildInstruction[];
         let carried: Record<string, ManifestEntry>;
         if (orchestrated) {
            instructions = opts.buildInstructions!;
            // Seed the build Manifest with the caller-supplied upstream
            // references so a downstream source's persist upstream (built in a
            // prior run, not rebuilt here) resolves to its existing physical
            // table instead of recomputing live. The reference key is the
            // compiler's manifest-lookup sourceEntityId (see ManifestReference).
            carried = this.referenceManifestToEntries(opts.referenceManifest);
            // Upstream resolution for an orchestrated build draws on three
            // sources, in DESCENDING precedence — a reference is an IDENTITY, not
            // a copy the caller must fully courier:
            //   1. The explicit `referenceManifest` couriered in THIS build call.
            //   2. The package's BOUND manifest (`manifestLocation`) — the set the
            //      orchestrator distributed to this worker. This is what makes the
            //      cross-worker flow work: a worker that never built the upstream
            //      still holds its full entry via the refreshed manifest, so a
            //      downstream can reuse the upstream's materialized table.
            //   3. This worker's own most-recent local manifest (skip-if-unchanged
            //      cache) — same-worker reuse.
            // Each fills the storage fields (sourceName, storageDestinationName,
            // schema) a thin reference can't carry — required for a `storage=`
            // upstream's stack-on-the-parent rebind. Higher-precedence
            // fields win; each step is best-effort (a fetch/read failure leaves
            // the entries as they are).
            await this.seedFromBoundManifest(carried, pkg, instructions);
            if (Object.keys(carried).length > 0) {
               await this.resolveReferencesFromStore(
                  carried,
                  environmentId,
                  packageName,
                  id,
               );
            }
         } else {
            // Skip-if-unchanged: reuse tables from the most recent successful
            // manifest for sources whose sourceEntityId is unchanged, unless
            // forceRefresh.
            const priorEntries = opts.forceRefresh
               ? {}
               : await this.getMostRecentManifestEntries(
                    environmentId,
                    packageName,
                    id,
                 );
            ({ instructions, carried } = this.deriveSelfInstructions(
               compiled,
               opts.sourceNames,
               priorEntries,
               incremental,
            ));
         }

         const { entries, failures } = await this.executeInstructedBuild(
            compiled,
            environment,
            instructions,
            carried,
            signal,
            opts.strictUpstreams ?? false,
            // Failure-path reclaim is ORCHESTRATED-ONLY on purpose — see
            // reclaimStorageTablesFromFailedRun.
            orchestrated ? { environmentId, packageName } : undefined,
            {
               // Optional for the same reason build_plan reads it optionally:
               // callers that build from a lighter package surface still resolve,
               // just without a package-level layer.
               packageMaterialization: pkg.getMaterializationConfig?.() ?? null,
               context: {
                  queryClass: "materialize",
                  environment: environmentName,
                  package: packageName,
                  // The caller's trigger wins because it can express a publish,
                  // which the publisher's own two-value trigger cannot.
                  trigger:
                     opts.runContext?.trigger ?? opts.trigger?.toLowerCase(),
                  // Default to the materialization id: the publisher always has a
                  // run id, so a build's statements group in the backend's query
                  // history whether or not the caller supplied one.
                  runId: opts.runContext?.runId ?? id,
               },
            },
            incremental,
         );

         const { sourcesBuilt, sourcesFailed, sourcesReused } = tallySources(
            entries,
            failures,
            carried,
         );
         const durationMs = Date.now() - startedAt;
         await this.commitManifest(id, entries, failures, {
            forceRefresh: opts.forceRefresh,
            sourceNames: opts.sourceNames ?? null,
            mode,
            trigger: opts.trigger,
            sourcesBuilt,
            sourcesReused,
            durationMs,
         });

         // Auto-run owns distribution: load the fresh manifest into the package
         // models so subsequent queries resolve to the materialized tables.
         // Orchestrated leaves distribution to the caller (manifestLocation).
         if (!orchestrated) {
            await this.autoLoadManifest(environment, packageName, entries);
         }

         recordSourcesOutcome("built", sourcesBuilt);
         recordSourcesOutcome("reused", sourcesReused);
         if (sourcesFailed > 0) {
            recordSourcesOutcome("failed", sourcesFailed);
         }
         // A run that lost sources is a partial success, not a success: the
         // manifest it committed is missing tables a consumer expected.
         this.recordRun(
            mode,
            sourcesFailed > 0 ? "partial" : "success",
            startedAt,
         );
         logger[sourcesFailed > 0 ? "warn" : "info"](
            sourcesFailed > 0
               ? "Materialization build complete with failed sources"
               : "Materialization build complete",
            {
               materializationId: id,
               packageName,
               mode,
               sourcesBuilt,
               sourcesReused,
               sourcesFailed,
               durationMs,
            },
         );
      } catch (err) {
         this.recordRun(mode, outcomeFor(err, signal), startedAt);
         throw err;
      }
   }

   /**
    * Derive the publisher's own build instructions for auto-run. Each persist
    * source (respecting the optional sourceNames filter) gets a self-assigned
    * physical table name and COPY realization, unless its sourceEntityId is unchanged
    * since `priorEntries` — those are carried forward (reused) instead of
    * rebuilt.
    *
    * An INCREMENTAL source is exempt from that carry-forward. Skip-if-unchanged
    * reuses a table because the SQL is unchanged, which is precisely the wrong
    * inference for a source whose whole premise is that the DATA moved while the
    * SQL stayed put — left in, an incremental source would be carried forward on
    * every non-forced run and its delta would never be reached. The exemption
    * costs nothing when there is no work: the ledger, not the content address,
    * decides, and planIncrementalStep answers "nothing new" with a skip that is
    * cheaper than the rebuild the carry was avoiding.
    */
   private deriveSelfInstructions(
      compiled: CompiledBuildPlan,
      sourceNames: string[] | undefined,
      priorEntries: Record<string, ManifestEntry>,
      incremental?: IncrementalRunContext,
   ): {
      instructions: BuildInstruction[];
      carried: Record<string, ManifestEntry>;
   } {
      const include = sourceNames ? new Set(sourceNames) : null;
      const instructions: BuildInstruction[] = [];
      const carried: Record<string, ManifestEntry> = {};
      const seen = new Set<string>();

      for (const graph of compiled.graphs) {
         for (const persistSource of iterGraphSources(
            graph,
            compiled.sources,
         )) {
            if (include && !include.has(persistSource.name)) continue;

            // Safety: a source that DECLARES `storage=` must never silently
            // downgrade to a colocated build when the external tier is disabled
            // (PERSIST_STORAGE_MODE=off). A colocated build writes a CTAS into the
            // source's OWN warehouse — which the author did not intend (they asked
            // for external storage; production grants this server read-only
            // warehouse access) and which could fail or land in an unexpected
            // schema. Skip it: the source is not materialized and serves LIVE, and
            // the mode warning (Package.storageWarnings) surfaces the degraded
            // state. A plain `#@ persist` (no `storage=`) is unaffected —
            // colocated IS its author's intent (the v0 path, ungated by the
            // storage kill switch).
            if (
               getPersistStorageMode() === "off" &&
               declaredStorage(persistSource)
            ) {
               continue;
            }

            const destination = resolveStorageDestination(persistSource);
            if (destination) {
               // Gate BEFORE computeSourceEntityId: an unbound parameter or a
               // given makes getSQL() (called inside computeSourceEntityId)
               // throw opaquely, so the eligibility refusal must fire first to
               // give a clean, actionable 422.
               assertMaterializationEligible(persistSource);
            } else {
               // No storage destination: this is the colocated `#@ persist`
               // path (a CTAS into the source's own warehouse). It is not
               // covered by assertMaterializationEligible above (that gate
               // only runs when a storage destination resolved), but it is
               // just as frozen as a storage build, so an authorize-gated
               // source materialized here would still be served to every
               // caller. Gate BEFORE computeSourceEntityId for the same
               // reason as the storage case above.
               //
               // The origin is passed so a REFUSED ROLLUP names `#@ preaggregate`
               // and not `#@ persist`: a rollup's name is synthesized and appears
               // nowhere in the author's model, so the default message sends them
               // hunting for a line they never wrote. See the function's doc.
               assertColocatedPersistNotAuthorizeGated(
                  persistSource,
                  persistSource.name,
                  compiled.preaggregatePlans?.[persistSource.sourceID]
                     ? "preaggregate"
                     : "persist",
                  compiled.sourceGateOutcomes?.[persistSource.sourceID],
               );
            }

            const sourceEntityId = computeSourceEntityId(
               persistSource,
               compiled.connectionDigests,
            );
            if (seen.has(sourceEntityId)) continue;
            seen.add(sourceEntityId);

            // Self-assign the physical name from `name=` (or the source name)
            // verbatim for BOTH the colocated and storage destinations — the only
            // difference between the two is which connection the table lands in. A
            // storage build replaces the table atomically (`CREATE OR REPLACE`),
            // so no generational decoration is needed to make a rebuild safe. An
            // orchestrated build ignores this and trusts the host-supplied
            // `physicalTableName`; the host owns any generational,
            // ownership-scoped naming.
            const logicalName = selfAssignTableName(persistSource);

            // Gated on the SAME predicate the build's dispatch uses, so the two
            // can never disagree about which sources are incremental. A
            // declaration the delta path cannot act on — an unsupported dialect,
            // an unusable watermark — returns undefined and keeps today's
            // skip-if-unchanged behavior rather than rebuilding fully on every
            // run in the name of an incremental refresh it will not perform.
            const deltaEligible =
               incremental !== undefined &&
               incrementalLineage({
                  declaration: incremental.declarations[persistSource.sourceID],
                  dialect: persistSource.dialectName,
                  targetDialect:
                     destination !== undefined
                        ? STORAGE_TARGET_DIALECT
                        : persistSource.dialectName,
                  physicalTableName: logicalName,
                  connectionName: persistSource.connectionName,
                  storageDestinationName: destination,
                  sourceEntityId,
               }) !== undefined;

            const prior = priorEntries[sourceEntityId];
            // Destination-scoped reuse: carry a prior table forward only when it
            // landed in the SAME destination. sourceEntityId is a pure content
            // address and does NOT encode the destination, so a source that adds,
            // drops, or switches `storage=` must rebuild — otherwise a
            // warehouse-landed (colocated) table would be silently reused for a
            // DuckLake serve that cannot resolve it.
            if (
               !deltaEligible &&
               prior &&
               prior.physicalTableName &&
               (prior.storageDestinationName ?? undefined) === destination
            ) {
               carried[sourceEntityId] = prior;
               continue;
            }

            instructions.push({
               sourceEntityId,
               materializedTableId: `local-${sourceEntityId.substring(
                  0,
                  STAGING_ID_LEN,
               )}`,
               physicalTableName: logicalName,
               realization: "COPY",
               ...(destination ? { destination } : {}),
            });
         }
      }

      return { instructions, carried };
   }

   /**
    * Project the caller-supplied upstream reference manifest into the seed
    * entry map `executeInstructedBuild` consumes. Each reference is keyed by the
    * compiler's manifest-lookup sourceEntityId and carries the physical table
    * name plus the connection it lives on — enough for the build Manifest to
    * resolve a downstream persist reference to the existing table, and to quote
    * that reference for the connection's dialect so it resolves on a
    * case-folding engine (see {@link quoteSeedTablePath}). `connectionName` is
    * optional on the wire: an older control plane that omits it seeds unquoted,
    * exactly as before.
    */
   private referenceManifestToEntries(
      referenceManifest: ManifestReference[] | undefined,
   ): Record<string, ManifestEntry> {
      const entries: Record<string, ManifestEntry> = {};
      for (const ref of referenceManifest ?? []) {
         entries[ref.sourceEntityId] = {
            sourceEntityId: ref.sourceEntityId,
            physicalTableName: ref.physicalTableName,
            connectionName: ref.connectionName,
         };
      }
      return entries;
   }

   /**
    * Resolve upstream references against this publisher's OWN most-recent
    * manifest, keyed by sourceEntityId (the same cache skip-if-unchanged reads).
    * A reference is fundamentally an IDENTITY, not a copy: for each one that
    * matches a locally persisted entry, fill in the fields a thin reference
    * can't carry (sourceName, storageDestinationName, schema — needed for a
    * `storage=` upstream's stack-on-the-parent rebind) from the local entry, while
    * letting any caller-supplied field WIN (the orchestrator is authoritative across a
    * stateless fleet). Mutates `carried` in place. Best-effort: a lookup failure
    * leaves the references exactly as supplied, so the cross-worker courier path
    * (a worker that never built the upstream, fed the full entry) is unaffected.
    */
   private async resolveReferencesFromStore(
      carried: Record<string, ManifestEntry>,
      environmentId: string,
      packageName: string,
      excludeId: string,
   ): Promise<void> {
      let cached: Record<string, ManifestEntry>;
      try {
         cached = await this.getMostRecentManifestEntries(
            environmentId,
            packageName,
            excludeId,
         );
      } catch (err) {
         logger.warn(
            "Reference resolve-local lookup failed; using references as supplied",
            {
               packageName,
               error: err instanceof Error ? err.message : String(err),
            },
         );
         return;
      }
      for (const [sourceEntityId, ref] of Object.entries(carried)) {
         const local = cached[sourceEntityId];
         if (!local) continue;
         // Local entry is the base; caller-supplied (defined) fields override it.
         const merged: ManifestEntry = { ...local };
         for (const [key, value] of Object.entries(ref)) {
            if (value !== undefined) {
               (merged as Record<string, unknown>)[key] = value;
            }
         }
         carried[sourceEntityId] = merged;
      }
   }

   /**
    * Seed upstream reuse from the package's BOUND manifest — the set the
    * orchestrator distributed to this worker via `manifestLocation`. This is the
    * cross-worker path: a worker that never built an upstream still holds its full
    * entry here (`storageDestinationName` + `schema` + `sourceName`), so a
    * downstream can reuse the upstream's materialized table instead of recomputing
    * it from raw. Adds any bound upstream not already carried (so a build needs no
    * explicit reference when the manifest is refreshed), and fills gaps in a
    * thin explicit reference — but an explicit `referenceManifest` field always
    * wins (it targets THIS build). Sources this build is producing are skipped
    * (built fresh, not reused). Best-effort: a fetch failure is ignored (the build
    * falls back to the local store / inline recompute). Mutates `carried`.
    */
   private async seedFromBoundManifest(
      carried: Record<string, ManifestEntry>,
      pkg: { getPackageMetadata(): { manifestLocation?: string | null } },
      instructions: BuildInstruction[],
   ): Promise<void> {
      const manifestLocation = pkg.getPackageMetadata().manifestLocation;
      if (!manifestLocation) return;
      let fetched;
      try {
         fetched = await fetchManifestEntries(manifestLocation);
      } catch (err) {
         logger.warn(
            "Build upstream resolution: bound manifest fetch failed; ignoring",
            {
               manifestLocation,
               error: err instanceof Error ? err.message : String(err),
            },
         );
         return;
      }
      // Reconstruct a ManifestEntry map from both tiers of the fetched manifest:
      // storage entries carry their full shape; colocated entries reconstruct from
      // the tableName manifest.
      const bound: Record<string, ManifestEntry> = {
         ...fetched.storageEntries,
      };
      for (const [eid, e] of Object.entries(fetched.tableNameManifest)) {
         if (!bound[eid]) {
            bound[eid] = {
               sourceEntityId: eid,
               physicalTableName: e.tableName,
               connectionName: e.connectionName,
            };
         }
      }
      const building = new Set(instructions.map((i) => i.sourceEntityId));
      for (const [eid, entry] of Object.entries(bound)) {
         if (building.has(eid)) continue;
         const existing = carried[eid];
         if (!existing) {
            carried[eid] = entry;
            continue;
         }
         // Explicit reference wins; the bound entry fills the gaps it left.
         const merged: ManifestEntry = { ...entry };
         for (const [key, value] of Object.entries(existing)) {
            if (value !== undefined) {
               (merged as Record<string, unknown>)[key] = value;
            }
         }
         carried[eid] = merged;
      }
   }

   /**
    * Entries of the most recent successful (MANIFEST_FILE_READY) materialization
    * for this package, used for skip-if-unchanged. Excludes the in-flight run.
    */
   private async getMostRecentManifestEntries(
      environmentId: string,
      packageName: string,
      excludeId: string,
   ): Promise<Record<string, ManifestEntry>> {
      const list =
         (await this.repository.listMaterializations(
            environmentId,
            packageName,
         )) ?? [];
      for (const m of list) {
         if (m.id === excludeId) continue;
         if (m.status === "MANIFEST_FILE_READY" && m.manifest?.entries) {
            // Legacy tolerance, removable with `ManifestEntry.error`: a manifest
            // written by 0.0.245-0.0.246 records a failed source among its
            // entries. Carrying one forward as reuse would retire the source from
            // every later run -- a warehouse error that clears on its own would
            // never be retried -- and seeding a downstream FROM from it would
            // compile against a table that was never created.
            return Object.fromEntries(
               Object.entries(m.manifest.entries).filter(
                  ([, entry]) => !isLegacyFailedEntry(entry),
               ),
            );
         }
      }
      return {};
   }

   /**
    * Load a freshly produced manifest into the package's models so persist
    * references resolve to the materialized tables. Best-effort: a load failure
    * is logged, not fatal (the run already reached MANIFEST_FILE_READY).
    */
   private async autoLoadManifest(
      environment: {
         reloadAllModelsForPackage(
            packageName: string,
            manifest: FreshnessManifest,
         ): Promise<void>;
         bindPackageStorageServeBindings(
            packageName: string,
            entries: Record<string, ManifestEntry>,
         ): Promise<void>;
      },
      packageName: string,
      entries: Record<string, ManifestEntry>,
   ): Promise<void> {
      // The post-build auto-load binds tableName-only entries: the control plane
      // stamps freshness (dataAsOf/window/fallback) on the wire manifest it
      // distributes, not on this in-memory post-build load, so these sources are
      // bound un-gated (always serve the freshly-built table).
      const manifestEntries: FreshnessManifest = {};
      for (const [sourceEntityId, entry] of Object.entries(entries)) {
         // Storage entries serve cross-connection via the virtual-source
         // bindings (below), NOT the same-connection manifest substitution —
         // putting one here would make the original model try to substitute the
         // source with a table on its OWN (source) connection, which doesn't
         // exist there. Only colocated entries go into the tableName manifest.
         if (entry.physicalTableName && !entry.storageDestinationName) {
            manifestEntries[sourceEntityId] = {
               tableName: entry.physicalTableName,
               // Carried so the bind step can quote the physical path for the
               // connection's dialect (Package.quoteBoundTableNames) — the
               // build CREATEd it quoted, so an unquoted read would miss on a
               // case-folding engine in the window before the control plane's
               // wire-manifest rebind.
               connectionName: entry.connectionName,
            };
         }
      }
      try {
         await environment.reloadAllModelsForPackage(
            packageName,
            manifestEntries,
         );
         // Separately bind the FULL entries as storage serve bindings — sources
         // materialized into a storage destination serve cross-connection via
         // the virtual-source transform, not the tableName manifest above. No-op
         // for a package with no storage= sources (deriveServeBindings → []).
         await environment.bindPackageStorageServeBindings(
            packageName,
            entries,
         );
         recordAutoLoadOutcome("success");
         logger.info("Auto-run: loaded manifest into package models", {
            packageName,
            entryCount: Object.keys(manifestEntries).length,
         });
      } catch (err) {
         recordAutoLoadOutcome("failure");
         logger.warn("Auto-run: failed to load manifest into package models", {
            packageName,
            error: err instanceof Error ? err.message : String(err),
         });
      }
   }

   /**
    * Validate caller-supplied build instructions against the package's compiled
    * build plan: every instructed sourceEntityId must be a planned source, and only
    * COPY realization is supported. Throws when the package declares no persist
    * source (no plan to build against).
    *
    * When the caller supplied the ledger, each of its entries is validated here
    * too, per the API contract that an invalid entry is a 400 rather than a
    * quiet rebuild: an entry must name a table this run instructs, name it once,
    * belong to a source that declares incremental refresh, and carry the content
    * address the plan carries for that source. The address check is the one
    * that fires in ordinary operation — a publish or rollback that changes a
    * source moves its address, and the caller's stored entry was measured under
    * the old one — which is exactly why it is checked HERE, synchronously,
    * where the caller can react (delete the entry, or set `reseed`), instead of
    * failing a background run. An entry for a source being reseeded is ignored
    * unvalidated: `reseed` is the documented recovery from this 400, so it must
    * not itself trip it.
    */
   private validateInstructions(
      plan: BuildPlan | null,
      instructions: BuildInstruction[],
      ledger?: LedgerEntry[],
      runReseed = false,
   ): void {
      if (!plan) {
         throw new BadRequestError(
            "Package has no persist sources; buildInstructions cannot be applied",
         );
      }
      const plannedSourceEntityIds = new Set<string>();
      for (const source of Object.values(plan.sources)) {
         plannedSourceEntityIds.add(source.sourceEntityId);
      }

      for (const instruction of instructions) {
         if (!plannedSourceEntityIds.has(instruction.sourceEntityId)) {
            throw new BadRequestError(
               `Instruction references unknown sourceEntityId '${instruction.sourceEntityId}'`,
            );
         }
         // COPY-only for now; SNAPSHOT lands once clone semantics are defined.
         if (instruction.realization === "SNAPSHOT") {
            throw new BadRequestError(
               "realization=SNAPSHOT is not supported (COPY only)",
            );
         }
      }

      if (ledger === undefined || ledger.length === 0) return;

      // What this run is building, by the table each instruction writes — the
      // same key a ledger entry carries — with what the plan knows about the
      // source writing it. Resolved by sourceID when the instruction echoes
      // one, else by content address (the same order the build loop matches
      // instructions in).
      const instructed = new Map<
         string,
         { sourceEntityId: string; refresh: string | null; reseed: boolean }
      >();
      // The same tables keyed WITHOUT their destination, which is what lets a
      // destination-less entry from an older caller be recognized as stale rather
      // than as naming a table this run does not build.
      const instructedIntoStorage = new Set<string>();
      const byAddress = new Map(
         Object.values(plan.sources).map((s) => [s.sourceEntityId, s]),
      );
      for (const instruction of instructions) {
         const planSource =
            (instruction.sourceID
               ? plan.sources[instruction.sourceID]
               : undefined) ?? byAddress.get(instruction.sourceEntityId);
         if (!planSource) continue; // Unreachable: every instruction matched above.
         instructed.set(
            tableKeyOf(
               planSource.connectionName,
               instruction.destination,
               instruction.physicalTableName,
            ),
            {
               sourceEntityId: planSource.sourceEntityId,
               refresh: planSource.refresh ?? null,
               reseed: runReseed || instruction.reseed === true,
            },
         );
         if (instruction.destination) {
            instructedIntoStorage.add(
               tableKeyOf(
                  planSource.connectionName,
                  undefined,
                  instruction.physicalTableName,
               ),
            );
         }
      }

      const seen = new Set<string>();
      for (const entry of ledger) {
         const key = tableKeyOf(
            entry.connectionName,
            entry.storageDestinationName,
            entry.physicalTableName,
         );
         const table =
            `'${entry.physicalTableName}' ` +
            (entry.storageDestinationName
               ? `in storage destination '${entry.storageDestinationName}'`
               : `on connection '${entry.connectionName}'`);
         if (seen.has(key)) {
            throw new BadRequestError(
               `Ledger entry for table ${table} appears more than once`,
            );
         }
         seen.add(key);
         const target = instructed.get(key);
         if (!target) {
            // A caller that predates `storageDestinationName` sends the entry
            // without it, and the table it names IS one this run builds — just in
            // a destination the entry cannot describe. That entry is STALE, not
            // wrong, so the source seeds (the index the build reads keys on the
            // destination too, so it finds no boundary) rather than the run being
            // refused. An entry naming a DIFFERENT destination is a caller error
            // and falls through to the refusal below.
            const storedHere = instructedIntoStorage.has(
               tableKeyOf(
                  entry.connectionName,
                  undefined,
                  entry.physicalTableName,
               ),
            );
            if (entry.storageDestinationName === undefined && storedHere) {
               continue;
            }
            throw new BadRequestError(
               `Ledger entry names table ${table}, which this run's ` +
                  `instructions do not build`,
            );
         }
         if (target.reseed) continue;
         if (target.refresh !== "incremental") {
            throw new BadRequestError(
               `Ledger entry names table ${table}, whose source does not ` +
                  `declare refresh="incremental"`,
            );
         }
         if (entry.sourceEntityId !== target.sourceEntityId) {
            throw new BadRequestError(
               `Ledger entry for table ${table} was measured under a ` +
                  `different definition of its source (its sourceEntityId does ` +
                  `not match the plan's) — expected after a publish or ` +
                  `rollback that changed the source. Delete the entry to ` +
                  `rebuild the source, or set reseed.`,
            );
         }
      }
   }

   /**
    * Quote a seeded upstream's physical path for the in-memory build Manifest,
    * mirroring the CREATE side ({@link quoteManifestTablePath}) so a downstream
    * persist source resolves the reference on a case-folding engine. The seed's
    * own connection (carried on the entry) supplies the dialect. If it can't be
    * resolved — an older control plane omitted `connectionName`, or the named
    * connection isn't part of this build — the path binds unquoted: the build
    * then fails loudly at the downstream CREATE on a case-folding engine (a
    * self-signaling miss, unlike the serve path), and is unaffected elsewhere.
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
         // A named-but-absent connection is a real gap (the seed can't be
         // quoted); a missing name is the benign older-CP default. Only the
         // former is worth a signal.
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
    * The run's incremental context, or undefined when no source in the package
    * asked for an incremental refresh — which is the common case, and which keeps
    * the build path byte-for-byte what it was.
    *
    * Resolved from the compiled plan rather than the wire plan because the
    * declaration is read off the compiled source (its output schema and query
    * definition), neither of which the wire plan carries.
    */
   private incrementalRunContext(
      compiled: CompiledBuildPlan,
      run: {
         environmentId: string;
         packageName: string;
         materializationId: string;
         forceRefresh: boolean;
         now: Date;
         /**
          * The caller-supplied ledger, when the caller owns it. Undefined means
          * the local store, as before; present (even empty) replaces it for the
          * whole run.
          */
         ledger: LedgerEntry[] | undefined;
      },
   ): IncrementalRunContext | undefined {
      // An unreadable declaration is dropped by collectIncrementalDeclarations
      // (with a warn), so that source builds full, like every other source whose
      // declaration the delta path cannot act on.
      const declarations: Record<string, IncrementalDeclaration> = {};
      for (const [sourceID, declaration] of Object.entries(
         collectIncrementalDeclarations(compiled.sources),
      )) {
         if (declaration.incremental) declarations[sourceID] = declaration;
      }
      if (Object.keys(declarations).length === 0) return undefined;
      const { ledger, ...rest } = run;
      return {
         ...rest,
         declarations,
         ledger: this.repository,
         ...(ledger !== undefined
            ? { callerLedger: indexCallerLedger(ledger) }
            : {}),
      };
   }

   /**
    * Shared build loop for both auto-run and orchestrated builds. Seeds the
    * manifest with carried-forward (reused) upstream entries so downstream
    * references resolve, then builds each instructed source in dependency
    * order. Returns the full entry map (carried + freshly built). The package
    * was compiled by the caller; this runs outside the package lock.
    */
   private async executeInstructedBuild(
      compiled: CompiledBuildPlan,
      environment: BuildEnvironment,
      instructions: BuildInstruction[],
      seedEntries: Record<string, ManifestEntry>,
      signal: AbortSignal,
      strict = false,
      // Identity of the run, used only to reclaim storage tables this run created
      // if it fails part-way (see reclaimStorageTablesFromFailedRun).
      owner?: { environmentId: string; packageName: string },
      buildMetadata?: BuildQueryMetadata,
      // The run's incremental context, when any source declared incremental
      // refresh. Undefined leaves every source on the full-rebuild path.
      incremental?: IncrementalRunContext,
   ): Promise<{
      entries: Record<string, ManifestEntry>;
      failures: Record<string, SourceFailure>;
   }> {
      const { graphs, sources, connectionDigests, connections } = compiled;

      // Index instructions by sourceID (the stable per-source handle) so the
      // build no longer recomputes the sourceEntityId to find an instruction.
      // Recomputing it here forced a caller's sourceEntityId to equal the publisher's
      // content hash, so a caller that derives sourceEntityIds by any other scheme
      // would have its sources silently skipped (the recomputed sourceEntityId would
      // not match the instruction). sourceEntityId is treated as opaque, caller-assigned
      // identity. A sourceEntityId index is kept as a fallback for instructions without
      // a sourceID (e.g. standalone auto-run).
      const bySourceID = new Map<string, BuildInstruction>();
      const bySourceEntityId = new Map<string, BuildInstruction>();
      for (const instruction of instructions) {
         if (instruction.sourceID) {
            bySourceID.set(instruction.sourceID, instruction);
         }
         bySourceEntityId.set(instruction.sourceEntityId, instruction);
      }

      // Accumulates physical names as sources are built so downstream sources
      // resolve their upstream references to the freshly-assigned tables. Seed
      // it with carried-forward entries so reused upstreams resolve too. In
      // strict mode, an upstream persist reference that is neither built here
      // nor seeded fails the compile (runtime-manifest-strict-miss) instead of
      // silently recomputing live.
      const manifest = new Manifest();
      manifest.strict = strict;
      const entries: Record<string, ManifestEntry> = {};
      for (const [sourceEntityId, entry] of Object.entries(seedEntries)) {
         if (entry.physicalTableName) {
            // The build Manifest feeds a downstream persist's `FROM` verbatim,
            // so a seeded upstream must carry the SAME quoting the builder
            // CREATEd it with — else the downstream CREATE misses the
            // case-preserved table on a case-folding engine. The seed keeps its
            // logical (unquoted) name in `entries` (the committed manifest, in
            // logical-name space); only the in-memory build Manifest is quoted.
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

      // Entries this run actually CREATED, as opposed to the seeded/carried ones
      // above. Only these are eligible for failure-path reclaim: a carried entry
      // names a table an earlier successful run built and a live manifest may still
      // serve, so dropping one would be data loss rather than cleanup.
      const builtThisRun: ManifestEntry[] = [];
      // Stored tables this run wrote and deliberately will NOT reclaim, so a
      // failure can name them. See reportRetainedStorageTables for why the list
      // cannot say which of them is actually unreferenced.
      const retainedThisRun: ManifestEntry[] = [];
      const failures: Record<string, SourceFailure> = {};
      const failedReasons: string[] = [];
      const builtSources: string[] = [];
      try {
         for (const graph of graphs) {
            const connection = connections.get(graph.connectionName);
            if (!connection) {
               throw new BadRequestError(
                  `Connection '${graph.connectionName}' not found`,
               );
            }
            for (const persistSource of iterGraphSources(graph, sources)) {
               if (signal.aborted) throw new Error("Build cancelled");

               // Prefer sourceID matching (so the caller's sourceEntityId scheme
               // stays opaque to the build); the sourceEntityId lookup below is the
               // fallback for instructions without a sourceID (auto-run, or an
               // orchestrated instruction that omits the optional `sourceID`).
               const orchestratedInstruction = bySourceID.get(
                  persistSource.sourceID,
               );

               // Resolve which instruction (if any) applies to this source
               // BEFORE any eligibility assert runs. A source with NO
               // instruction — a refused sibling the caller never asked to
               // build, alongside others this run does build — must be
               // skipped here rather than examined: the asserts below throw a
               // 422, and unconditionally running them on every persist
               // source in the graph (regardless of whether it had a build to
               // do at all) meant one refused, uninstructed sibling aborted
               // the whole run instead of just the sources actually
               // instructed.
               let sourceEntityId: string | undefined;
               let instruction = orchestratedInstruction;
               if (!instruction) {
                  // computeSourceEntityId calls getSQL(), which throws
                  // opaquely for a free-parameter or given source —
                  // precisely the sources a caller can never legitimately
                  // hold a sourceEntityId for in the first place (the wire
                  // build plan omits such a source's entry entirely), so
                  // they can never match here regardless. Treat that throw
                  // the same as "no match" rather than letting it escape and
                  // abort every other instructed source.
                  try {
                     sourceEntityId = computeSourceEntityId(
                        persistSource,
                        connectionDigests,
                     );
                     instruction = bySourceEntityId.get(sourceEntityId);
                  } catch {
                     instruction = undefined;
                  }
               }
               if (!instruction) continue;

               // Enforce the eligibility gate for any storage-targeted build,
               // including orchestrated (host-supplied) instructions — the publisher
               // refuses an ineligible source into the tier itself, not on trust.
               // Skipped when the mode is off: the refusal below owns that case.
               if (
                  orchestratedInstruction?.destination &&
                  getPersistStorageMode() !== "off"
               ) {
                  assertMaterializationEligible(persistSource);
               } else {
                  // The gate refusal above only fires for a STORAGE-targeted
                  // build, so on its own it leaves every other instruction —
                  // the colocated one with no destination, and any build while
                  // the mode is off — unexamined. An orchestrated host chooses
                  // the destination, so this path reaches that case with no
                  // `#@ persist`-vs-`storage=` distinction to lean on; refuse
                  // a gated source however it was instructed, unless its
                  // compile-time gate outcome proves the colocated relaxation
                  // applies (see `assertColocatedPersistNotAuthorizeGated`'s
                  // doc). `else` rather than an unconditional call:
                  // `assertMaterializationEligible` already runs the identical
                  // `referencesAuthorize` IR walk, so calling both on the
                  // storage path would walk every persist source's whole
                  // `SourceDef` twice per build for an answer the first call
                  // has already acted on.
                  assertColocatedPersistNotAuthorizeGated(
                     persistSource,
                     persistSource.name,
                     compiled.preaggregatePlans?.[persistSource.sourceID]
                        ? "preaggregate"
                        : "persist",
                     compiled.sourceGateOutcomes?.[persistSource.sourceID],
                  );
               }

               // The manifest is keyed by the content sourceEntityId — what Malloy
               // recomputes to resolve upstream persist references during SQL
               // generation — independent of the instruction's identity sourceEntityId.
               // Already computed above when this source matched via the
               // sourceEntityId fallback; a sourceID match skips that path, so
               // compute it now that eligibility has cleared.
               sourceEntityId ??= computeSourceEntityId(
                  persistSource,
                  connectionDigests,
               );

               // A caller-instructed destination that cannot be honored REFUSES; it
               // never falls through to a colocated build. Auto-run already applies
               // this rule, by skipping a `storage=` source while the tier is off
               // (see deriveSelfInstructions), for the reason given there: a
               // colocated build CTASes into the source's OWN warehouse, which the
               // author asked this data not to be written to. An instructed build
               // needs the rule more, not less — the caller named a destination AND
               // a physical name, so falling through writes that name into the
               // warehouse and answers success, and the caller can only detect it by
               // comparing the echoed destination against the one it sent.
               //
               // Refused rather than skipped because the caller asked for a specific
               // table: silence reads as "built". Stepping the mode down is a
               // rollback, so it has to stay safe on a loaded package — declining a
               // write is safe, redirecting one is not. Auto-run cannot reach this:
               // resolveStorageDestination returns undefined while off, so an
               // instruction still carrying a destination here is caller-supplied.
               if (
                  instruction.destination &&
                  getPersistStorageMode() === "off"
               ) {
                  throw new BadRequestError(
                     `Source '${persistSource.name}' was instructed to build into ` +
                        `storage destination '${instruction.destination}', but ` +
                        `PERSIST_STORAGE_MODE is off, so no destination can be ` +
                        `written. Refusing rather than building the table into the ` +
                        `source warehouse instead.`,
                  );
               }

               // Auto-run already gated pre-getSQL in deriveSelfInstructions;
               // re-assert (idempotent) so no path into a storage build is ungated.
               if (
                  !orchestratedInstruction &&
                  instruction.destination &&
                  getPersistStorageMode() !== "off"
               ) {
                  assertMaterializationEligible(persistSource);
               }

               let entry;
               try {
                  entry = await this.buildOneSource(
                     persistSource,
                     instruction,
                     connection,
                     connectionDigests,
                     manifest,
                     environment,
                     entries,
                     buildMetadata,
                     incremental,
                     // The CONTENT address, distinct from the instruction's
                     // caller-assigned identity: the ledger is keyed by it so a
                     // boundary can never be read against different SQL.
                     sourceEntityId,
                  );
               } catch (buildErr) {
                  // One source failing does not end the build: the sources that
                  // did materialize stay usable, and this one records the reason
                  // it gave so a consumer can attribute the failure to the unit it
                  // belongs to rather than to the whole command. A build that
                  // loses every source still fails, below.
                  //
                  // Redacted against this source's connection CONFIG, not the
                  // live connection: a warehouse error can echo the credentials it
                  // was handed, and this value is persisted. The config is where
                  // the declared secrets are, and it is what every other redaction
                  // site passes. Walking a live connection instead is both
                  // unnecessary and unsound -- an enumerable accessor that throws
                  // once its resource is gone (the state a build failure runs in)
                  // escapes the walk and fails the whole run, taking the sources
                  // that did materialize with it, and state held in a Map is not
                  // walked at all, so a secret there passes through in the clear.
                  //
                  // A connection missing from the environment is not a reason to
                  // leak: getApiConnection throws, so fall back to the
                  // connection-free string redactor that redactConnectionSecrets
                  // applies anyway.
                  let reason: string;
                  try {
                     reason = redactConnectionSecrets(
                        errMessage(buildErr),
                        environment.getApiConnection(
                           persistSource.connectionName,
                        ),
                     );
                  } catch {
                     reason = redactPgSecrets(errMessage(buildErr));
                  }
                  // The manifest carries this reason to the control plane, but a
                  // build that lost a source no longer throws -- so without a log
                  // here the failure is invisible to anyone reading the server's
                  // own output.
                  logger.warn("Source failed to materialize", {
                     packageName: owner?.packageName,
                     sourceName: persistSource.name,
                     physicalTableName: instruction.physicalTableName,
                     reason,
                  });
                  failedReasons.push(`${persistSource.name}: ${reason}`);
                  failures[sourceEntityId] = {
                     sourceEntityId,
                     sourceName: persistSource.name,
                     materializedTableId: instruction.materializedTableId,
                     physicalTableName: instruction.physicalTableName,
                     reason,
                     // The destination discriminator: without it, a consumer
                     // holding only this failure cannot tell a colocated
                     // failure from a `storage=` one and, computing
                     // `storageDestinationName ?? connectionName` the same
                     // way a successful build's ManifestEntry does, would
                     // resolve the wrong key.
                     connectionName: persistSource.connectionName,
                     ...(instruction.destination
                        ? { storageDestinationName: instruction.destination }
                        : {}),
                  };
                  // Mirrored into `entries` for the deprecation window, because
                  // consumers built against 0.0.245-0.0.246 read the failure from
                  // `ManifestEntry.error` and would otherwise see this source as
                  // merely ABSENT -- which reads as "nothing to report" rather
                  // than "this failed", the fail-dangerous direction. Written
                  // unconditionally rather than merged onto whatever is already
                  // here: a source can be BOTH seeded (from a reference manifest
                  // or bound manifest, which apply no exclusion keyed on this
                  // run's content address) and instructed-and-failed, and the
                  // seed names the PRIOR generation's table -- a real table,
                  // which a serve binding would resolve and serve as fresh.
                  // Overwriting is what makes `error` present on the entry any
                  // such consumer reads. Remove this whole block with the field.
                  entries[sourceEntityId] = {
                     sourceEntityId,
                     sourceName: persistSource.name,
                     physicalTableName: instruction.physicalTableName,
                     materializedTableId: instruction.materializedTableId,
                     error: reason,
                  } as ManifestEntry;
                  continue;
               }
               builtSources.push(persistSource.name);
               entries[sourceEntityId] = entry;
               if (isReclaimableStorageTable(entry)) {
                  builtThisRun.push(entry);
               } else if (entry.storageDestinationName) {
                  // Kept so a failed run can SAY what it left behind. Recorded
                  // here rather than logged here because a run that goes on to
                  // succeed leaves nothing behind at all — its manifest names
                  // every one of these.
                  retainedThisRun.push(entry);
               }
            }
         }

         // Every source this run built failed, so it produced nothing. A build
         // with no output must not report itself as one that succeeded with
         // errors attached; the partial path above covers the case where at least
         // one source is usable. Thrown from inside the try so a run that wrote
         // nothing reclaimable still takes the same cleanup path as any other
         // total failure.
         if (builtSources.length === 0 && failedReasons.length > 0) {
            throw new Error(failedReasons.join("; "));
         }
      } catch (err) {
         // A part-way failure returns a manifest that records the sources which
         // built and the reason each failed one gave, so those tables stay
         // reachable to manifest-driven GC. This path is for a build that
         // produced nothing to record -- an orchestration failure, or every
         // source failing -- where a table an earlier source wrote would
         // otherwise be unreachable forever. Reclaim those before rethrowing.
         // Best-effort and non-fatal: the build's own failure is what the caller
         // needs to see.
         if (owner) {
            reportRetainedStorageTables(retainedThisRun, owner.packageName);
            await this.reclaimStorageTablesFromFailedRun(
               builtThisRun,
               environment,
               owner,
            );
         }
         throw err;
      }

      return { entries, failures };
   }

   /**
    * Drop the storage tables a FAILED run created, so a partial build does not
    * leak an unreferenced table. For DuckLake that is data plus Parquet files at
    * rest, and nothing else will ever name them: the run commits no manifest, and
    * GC reclaims only what a manifest records.
    *
    * Three guards make this safe, and each closes a real way to destroy live data:
    *
    * - ORCHESTRATED runs only (the caller passes no `owner` for auto-run). Those
    *   names are host-assigned and generational, so unique by construction — which
    *   is both where the leak actually bites and the only case where a drop cannot
    *   hit something another run owns. Auto-run's STABLE names are overwritten in
    *   place by the next build, so skipping them forgoes little.
    *
    *   This gate is what bounds the cross-environment hazard. The
    *   still-referenced check below reads THIS environment and package only, and a
    *   BuildID carries no environment input, so two environments sharing a
    *   destination can resolve a source to the SAME physical name. A reclaim that
    *   trusted a per-environment check could then drop a table another environment
    *   is actively serving — the failure mode behind a real cross-environment
    *   data-loss incident on the hosted side. Generational names remove the
    *   collision rather than racing it. The durable fix is refusing a colliding
    *   persist target at validation time; until then, do not widen this.
    *
    * - Only entries this run CREATED (never a carried-forward one).
    * - Only names no other MANIFEST_FILE_READY run references, the same
    *   destination-and-name check {@link dropMaterializedTables} applies.
    *
    * Scoped to `storage=` entries. A colocated failure is left alone: those names
    * are stable and in the customer's own warehouse, so the next successful build
    * overwrites in place and a failure-path DROP there would be a far larger
    * blast radius for no reclaim.
    */
   /**
    * The destination's config for a best-effort table drop, or undefined when it
    * is no longer configured.
    *
    * The cleanup sweeps must keep going over the rest of a manifest rather than
    * abort on the first destination that has since been un-registered: a table
    * left behind is reclaimable later, whereas an aborted sweep leaves every
    * table after it behind too, and — in the delete path — throws out of a
    * sweep documented as best-effort. The build path deliberately does NOT go
    * through here; there, an unresolvable destination must fail the run.
    */
   private destinationForCleanup(
      environment: BuildEnvironment,
      destinationName: string,
   ): ApiConnection | undefined {
      try {
         return environment.getStorageDestination(destinationName);
      } catch (error) {
         logger.warn(
            "Skipping a table drop: its storage destination is no longer configured",
            { destinationName, error: errMessage(error) },
         );
         return undefined;
      }
   }

   private async reclaimStorageTablesFromFailedRun(
      builtThisRun: ManifestEntry[],
      environment: BuildEnvironment,
      owner: { environmentId: string; packageName: string },
   ): Promise<void> {
      if (builtThisRun.length === 0) return;
      try {
         const tableKey = (dest: string, table: string) => `${dest}:${table}`;
         const stillReferenced = new Set<string>();
         const others =
            (await this.repository.listMaterializations(
               owner.environmentId,
               owner.packageName,
            )) ?? [];
         for (const other of others) {
            if (other.status !== "MANIFEST_FILE_READY") continue;
            for (const e of Object.values(other.manifest?.entries ?? {})) {
               const dest = e.storageDestinationName ?? e.connectionName;
               if (dest && e.physicalTableName) {
                  stillReferenced.add(tableKey(dest, e.physicalTableName));
               }
            }
         }

         for (const entry of builtThisRun) {
            const dest = entry.storageDestinationName;
            const table = entry.physicalTableName;
            if (!dest || !table) continue;
            if (stillReferenced.has(tableKey(dest, table))) {
               logger.info(
                  "Keeping a table from a failed run: a live manifest still serves it",
                  { destinationName: dest, physicalTableName: table },
               );
               continue;
            }
            const destinationConnection = this.destinationForCleanup(
               environment,
               dest,
            );
            if (!destinationConnection) continue;
            try {
               await dropStorageTable({
                  destinationName: dest,
                  destinationConnection,
                  physicalTableName: table,
                  environmentPath: environment.getEnvironmentPath(),
               });
               recordDropTables("success", "storage");
               logger.info("Reclaimed a table stranded by a failed build", {
                  destinationName: dest,
                  physicalTableName: table,
               });
            } catch (dropErr) {
               recordDropTables("failure", "storage");
               logger.warn(
                  "Failed to reclaim a table stranded by a failed build",
                  {
                     destinationName: dest,
                     physicalTableName: table,
                     error: redactConnectionSecrets(
                        errMessage(dropErr),
                        destinationConnection,
                     ),
                  },
               );
            }
         }
      } catch (err) {
         // Never let cleanup mask the build failure that triggered it.
         logger.warn("Failed-run table reclaim did not complete", {
            error: errMessage(err),
         });
      }
   }

   /**
    * The `RunSQLOptions` for one source's build statements: its resolved
    * per-query metadata, merged under this run's context.
    *
    * Layers, least specific first: the executing connection's default, then what
    * the model side declared for this source (package → model-file → `#@ persist`,
    * already resolved by {@link resolveQueryMetadata}), then the run's context,
    * which names the source. There is no request layer — a build has no request.
    *
    * Fails OPEN: a build must not fail because metadata could not be assembled,
    * so an unresolvable connection just contributes no default, and a dropped
    * property is logged and metered rather than thrown.
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

   /**
    * The `RunSQLOptions` for the drops that retire a materialization's tables.
    * No model layer: the source's declaration described how to BUILD it, and the
    * source may no longer exist by the time its table is retired.
    */
   private dropRunSQLOptions(
      environment: BuildEnvironment,
      connectionName: string,
      environmentName: string,
      packageName: string,
      materializationId: string,
   ): { queryMetadata?: QueryMetadata } {
      const connectionLayers = connectionMetadataLayers(
         environment,
         connectionName,
      );
      const resolved = mergeQueryMetadata({
         connection: connectionLayers.default,
         enforced: connectionLayers.enforced,
         context: {
            queryClass: "ops",
            environment: environmentName,
            package: packageName,
            runId: materializationId,
         },
      });
      return resolved.metadata ? { queryMetadata: resolved.metadata } : {};
   }

   /**
    * Build a single instructed source into its assigned physical table.
    * COPY uses a staging table + atomic rename for crash-safety; the staging
    * name derives from the sourceEntityId. Records and returns the manifest entry.
    */
   private async buildOneSource(
      persistSource: PersistSource,
      instruction: BuildInstruction,
      connection: MalloyConnection,
      connectionDigests: Record<string, string>,
      manifest: Manifest,
      environment: BuildEnvironment,
      builtEntries: Record<string, ManifestEntry>,
      buildMetadata?: BuildQueryMetadata,
      // Present only when the run resolved at least one incremental declaration;
      // absent leaves this method exactly the full-rebuild path it was.
      incremental?: IncrementalRunContext,
      // The source's content address — NOT `instruction.sourceEntityId`, which is
      // caller-assigned and opaque (see the index comment in
      // executeInstructedBuild). Recorded on the covered_through ledger and
      // COMPARED on every read, which is what guarantees a boundary is never read
      // against SQL other than the SQL it was computed from: different SQL is a
      // mismatch, so a changed source seeds instead of deltaing onto a stale table.
      // (It used to be part of the ledger's key, which gave the same guarantee for
      // free but made a boundary un-findable across a package's versions.)
      contentSourceEntityId?: string,
   ): Promise<ManifestEntry> {
      const sourceEntityId = instruction.sourceEntityId;
      const physicalTableName = instruction.physicalTableName;
      const isStorageBuild =
         !!instruction.destination && getPersistStorageMode() !== "off";
      // ANY warehouse-executed build SQL — a colocated CTAS or a storage build's
      // native passthrough — runs against the SOURCE warehouse, which cannot see
      // a storage-materialized upstream's DuckDB/DuckLake table (a different
      // engine). Substituting that upstream's lake table name into warehouse SQL
      // would reference a table the warehouse can't resolve (a confusing "table
      // not found"), whether the downstream is a storage build OR a colocated
      // source reading a storage upstream. So exclude storage-materialized
      // upstreams from the build manifest in BOTH cases: non-strict, they INLINE
      // (recompute from raw against the warehouse) so a chained source still
      // materializes; under `strictUpstreams` the excluded reference becomes a
      // clean strict-miss error (the orchestrated contract — don't silently
      // recompute). A stack-on-the-parent build reads the parent's lake table
      // instead, but via a separate DuckDB recompile that does NOT use this
      // warehouse buildSQL — this remains its recompute-from-raw fallback.
      //
      // Which is why a storage build generates it permissively. For that build this
      // SQL is only the fallback, so refusing to GENERATE it refuses the source
      // before the parent-reuse attempt below can run: the strict-miss fires on SQL
      // nothing was going to execute, and a chained storage source becomes
      // unbuildable under `strictUpstreams` even though reading the parent needs no
      // recompute at all. Strict is enforced where a recompute would really happen —
      // buildOneSourceIntoStorage refuses if stacking on the parent proves
      // impossible. A colocated build keeps strict here: its buildSQL IS what runs,
      // and it has no parent to stack on.
      const reducedManifest = manifestExcludingStorage(manifest, builtEntries);
      const buildManifest = isStorageBuild
         ? { ...reducedManifest, strict: false }
         : reducedManifest;
      const buildSQL = persistSource.getSQL({
         buildManifest,
         connectionDigests,
      });

      // Every statement of this source's build carries the same metadata, so the
      // warehouse's query history shows the staging CTAS, the drop and the rename
      // as one attributable unit of work.
      //
      // Resolved before the storage branch below so BOTH build paths carry the
      // same bag, layered the same way. A `storage=` build reads through DuckDB's
      // query-passthrough rather than a Malloy connection, so it applies these
      // itself instead of handing them to `runSQL` — but what it applies has to be
      // the same properties, or one deployment's query history attributes the two
      // paths differently.
      const runOptions = this.buildRunSQLOptions(
         persistSource,
         environment,
         buildMetadata,
      );

      // `storage=` build: materialize into a DuckDB/DuckLake destination via a
      // build-scoped session (never on the source or serve connection). Diverges
      // fully from the in-warehouse CTAS below — different engine, credential
      // federation, and a captured authoritative schema for the serve transform.
      // Gated by the kill switch: when off, ignore a destination and do a colocated build.
      if (isStorageBuild) {
         // Stack-on-the-parent detection: does the source's SQL change when storage upstreams
         // are PRESENT in the manifest (mapped to their lake tables) vs EXCLUDED
         // (inlined, the buildSQL above)? If so it reads a storage-materialized
         // upstream, so it can be built by reading the parent's lake table
         // ("stack on the parent") instead of recomputing from raw. The compare
         // is graph-free and self-contained; a single-source build's two SQLs are
         // identical, so it skips straight to the passthrough below.
         const dependsOnStorageUpstream =
            persistSource.getSQL({
               buildManifest: manifest.buildManifest,
               connectionDigests,
            }) !== buildSQL;
         // Materialize ONLY the source's PUBLIC columns. `getSQL` projects every
         // underlying column, including ones the source hides (`except:`, non-public
         // access modifiers). Query reachability is bounded by the declared
         // ::Shape, which the serve transform narrows to the public surface
         // (proven by the shape-bounds-physical-columns scenario) — so what this
         // prevents is the hidden column's VALUES sitting at rest in the
         // destination store, reachable by direct catalog access and possibly
         // across a trust boundary the source's visibility was meant to hold.
         // Refuses the build (422) if the public surface can't be determined.
         const publicBuildSQL = projectToPublicColumns(persistSource, buildSQL);
         return this.buildOneSourceIntoStorage({
            persistSource,
            instruction,
            manifest,
            environment,
            publicBuildSQL,
            // The UNprojected form as well, for a delta: it applies the public
            // projection outermost, around its own range predicate, so the two
            // write the same columns without the predicate having to survive a
            // projection that may not carry the watermark.
            buildSQL,
            builtEntries,
            dependsOnStorageUpstream,
            queryMetadata: runOptions.queryMetadata,
            incremental,
            contentSourceEntityId,
         });
      }

      // Incremental refresh: a source that declares `refresh="incremental"` and a
      // usable watermark can advance its serving table by a bounded delta instead
      // of being rebuilt below. Every source that declares nothing — and every
      // one whose declaration the delta path cannot act on — gets `undefined`
      // here and falls straight through to the CTAS, unchanged.
      const dialect = persistSource.dialectName;
      const quotedPhysicalPath = quoteTablePath(physicalTableName, dialect);
      // Colocated by construction: the storage branch above has already returned,
      // so the table lives in the source's own warehouse and one dialect answers
      // both halves.
      const lineage =
         incremental && contentSourceEntityId
            ? incrementalLineage({
                 declaration: incremental.declarations[persistSource.sourceID],
                 dialect,
                 targetDialect: dialect,
                 physicalTableName,
                 connectionName: persistSource.connectionName,
                 sourceEntityId: contentSourceEntityId,
              })
            : undefined;
      // One narrowing for the three incremental touchpoints below, so they can
      // never disagree about whether this source is on the delta path.
      const incrementalRefresh =
         incremental && lineage && contentSourceEntityId
            ? {
                 context: incremental,
                 lineage,
              }
            : undefined;
      if (incrementalRefresh) {
         const applied = await this.refreshOneSourceIncrementally({
            ...incrementalRefresh,
            persistSource,
            instruction,
            target: warehouseDeltaTarget({
               dialect,
               runner: (sql) => connection.runSQL(sql, runOptions),
               quotedTablePath: quotedPhysicalPath,
               lineage: incrementalRefresh.lineage,
               // The CTAS's own SQL, manifest-resolved. The delta filters this
               // exact string, so it computes what a rebuild would — see
               // deltaSelect.
               sourceSQL: buildSQL,
            }),
            manifest,
         });
         if (applied) return applied;
      }

      const stagingTableName = `${physicalTableName}${stagingSuffix(sourceEntityId)}`;
      // The control plane sends the logical (unquoted) physical name; dialect-
      // quote each identifier here so a container path or quote-requiring name
      // (e.g. a hyphenated BigQuery project id) produces valid DDL. The manifest
      // echoes the logical name (below) so the CP stays in logical-name space.
      const quotedStaging = quoteTablePath(stagingTableName, dialect);
      const quotedPhysical = quotedPhysicalPath;
      // Not the bare name unconditionally: on a dialect that resolves an
      // unqualified rename target against the session, a bare target moves the
      // finished table into the session's default container.
      const quotedRenameTarget = quoteRenameTarget(physicalTableName, dialect);

      // The rebuild below replaces the table the boundary describes, so the
      // boundary is dropped BEFORE it starts rather than overwritten after: a
      // crash mid-rebuild would otherwise leave a boundary pointing at data that
      // no longer exists, and the next run would apply a delta on top of it.
      if (incrementalRefresh) {
         await resetLedger(
            incrementalRefresh.context,
            incrementalRefresh.lineage,
         );
      }

      const startTime = performance.now();
      await connection.runSQL(
         `DROP TABLE IF EXISTS ${quotedStaging}`,
         runOptions,
      );
      // The CTAS is the only statement here that reads the source data, so it is
      // the only one whose cost is worth reporting: the DROPs and the RENAME are
      // metadata operations. Undefined on any backend that does not report a
      // figure — see the manifest entry's queryCostBytes.
      let buildCostBytes: number | undefined;
      try {
         const ctas = await connection.runSQL(
            `CREATE TABLE ${quotedStaging} AS (${buildSQL})`,
            runOptions,
         );
         buildCostBytes = ctas?.runStats?.queryCostBytes;
         await connection.runSQL(
            `DROP TABLE IF EXISTS ${quotedPhysical}`,
            runOptions,
         );
         await connection.runSQL(
            `ALTER TABLE ${quotedStaging} RENAME TO ${quotedRenameTarget}`,
            runOptions,
         );
      } catch (err) {
         try {
            await connection.runSQL(
               `DROP TABLE IF EXISTS ${quotedStaging}`,
               runOptions,
            );
         } catch (cleanupErr) {
            logger.warn(
               "Failed to clean up staging table after a failed build; physical leak",
               {
                  stagingTableName,
                  connectionName: persistSource.connectionName,
                  cleanupError: errMessage(cleanupErr),
               },
            );
         }
         throw err;
      }

      // Make this table visible to downstream sources built later in this run.
      // Record the SAME quoted path the CREATE used (not the logical name): the
      // build Manifest is pasted into a downstream `FROM` verbatim, so on a
      // case-folding engine an unquoted name would miss the case-preserved table
      // just written. The returned entry (below) keeps the logical name for the
      // committed manifest; only this in-memory build Manifest is quoted.
      manifest.update(sourceEntityId, { tableName: quotedPhysical });

      const durationMs = Math.round(performance.now() - startTime);
      recordSourceBuildDuration(durationMs, "in_warehouse");
      logger.info(`Built materialized source ${persistSource.name}`, {
         physicalTableName,
         durationMs,
      });

      // The table now holds a full snapshot, so record where that snapshot
      // reaches: this is what turns the NEXT refresh into a delta.
      const seededThrough = incrementalRefresh
         ? await advanceLedgerAfterSeed({
              context: incrementalRefresh.context,
              lineage: incrementalRefresh.lineage,
              quotedTablePath: quotedPhysical,
              dialect,
              runner: (sql) => connection.runSQL(sql, runOptions),
           })
         : undefined;

      return {
         sourceEntityId,
         sourceName: persistSource.name,
         materializedTableId: instruction.materializedTableId,
         physicalTableName,
         connectionName: persistSource.connectionName,
         realization: instruction.realization,
         // The table now holds a full snapshot, so the boundary probed from it
         // is what turns the NEXT refresh into a delta.
         ...ledgerFields(incrementalRefresh?.lineage, seededThrough),
         // Reported for an incremental source whatever brought it here — no
         // recorded boundary, `reseed`, a table the delta path could not build
         // on — because from outside, a rebuild in the name of an incremental
         // refresh is otherwise indistinguishable from a delta.
         ...refreshFields(incrementalRefresh ? "full" : undefined),
         rowCount: null,
         buildDurationMs: durationMs,
         // The recurring warehouse cost of keeping this source materialized —
         // the debit against whatever the materialization saves on the read side.
         queryCostBytes: buildCostBytes ?? null,
      };
   }

   /**
    * Advance one source's serving table by a bounded delta, if that is what this
    * refresh should do.
    *
    * Returns the source's manifest entry when the table was advanced in place (or
    * needed nothing), and `undefined` to mean "rebuild it" — in which case the
    * caller falls through to the CTAS below, which is the ordinary full build.
    * Every reason for that fallback is logged and counted by
    * {@link reportIncrementalStep}.
    *
    * The DML runs on the SOURCE connection with the same run options as the rest
    * of the build, so a delta shows up in the warehouse's query history as part of
    * the same attributable unit of work as the seed it replaces.
    */
   private async refreshOneSourceIncrementally(params: {
      context: IncrementalRunContext;
      lineage: IncrementalLineage;
      persistSource: PersistSource;
      instruction: BuildInstruction;
      target: DeltaTarget;
      manifest: Manifest;
   }): Promise<ManifestEntry | undefined> {
      const { context, lineage, persistSource, instruction, target } = params;
      const sourceEntityId = instruction.sourceEntityId;

      const step = await planSourceRefresh({
         context,
         lineage,
         target,
         columns: deriveColumns(persistSource).map((c) => String(c.name)),
         reseed: instruction.reseed,
      });
      const outcome = await applyIncrementalStep({
         context,
         lineage,
         step,
         target,
         sourceName: persistSource.name,
      });
      if (!outcome.applied) return undefined;
      if (outcome.durationMs !== undefined) {
         recordSourceBuildDuration(outcome.durationMs, "delta");
      }

      // Both a delta and a skip leave the existing table serving, so it still has
      // to appear in the build manifest: a downstream source built later in this
      // run resolves its upstream through here, and an absent entry would make it
      // recompute the upstream from raw instead.
      params.manifest.update(sourceEntityId, {
         tableName: target.quotedTablePath,
      });
      return {
         sourceEntityId,
         sourceName: persistSource.name,
         materializedTableId: instruction.materializedTableId,
         physicalTableName: lineage.physicalTableName,
         connectionName: persistSource.connectionName,
         realization: instruction.realization,
         rowCount: null,
         buildDurationMs: outcome.durationMs ?? null,
         // The delta script runs through applyDeltaScript rather than a single
         // connection.runSQL whose result reaches here, so there is no cost figure
         // to report even on a backend that would supply one.
         queryCostBytes: null,
         // A delta reports where it advanced to; a skip reports the boundary
         // that stays in force. Either way the caller reads coverage from the
         // entry rather than inferring it from the run's outcome.
         ...ledgerFields(lineage, outcome.coveredThrough),
         // A skip applied nothing and says so, rather than leaving a reader to
         // infer it from an absent field: its unchanged `ledger` boundary
         // already says the table stands where it did.
         ...refreshFields(outcome.refresh),
      };
   }

   /**
    * Materialize a source into a `storage=` destination (a DuckDB/DuckLake
    * connection) via a native query-passthrough CTAS on a build-scoped session.
    * Records the destination connection and the captured authoritative DuckDB
    * schema on the manifest entry so the source can later be served
    * cross-dialect from the destination (the serve transform declares that
    * schema). `connectionName` still names the SOURCE warehouse (where data is
    * read from); `storageDestinationName` names where the table now lives.
    */
   private async buildOneSourceIntoStorage(params: {
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
      /**
       * Applied to the warehouse read by the passthrough itself — see
       * {@link buildSourceIntoStorage}. Resolved by the caller through the same
       * layering the colocated path uses.
       */
      queryMetadata?: QueryMetadata;
      /** Present when any source in the run declared an incremental refresh. */
      incremental?: IncrementalRunContext;
      /** The source's CONTENT address — see buildOneSource's parameter of the same name. */
      contentSourceEntityId?: string;
   }): Promise<ManifestEntry> {
      const {
         persistSource,
         instruction,
         manifest,
         environment,
         builtEntries,
         dependsOnStorageUpstream,
         queryMetadata,
         incremental,
      } = params;
      const sourceEntityId = instruction.sourceEntityId;
      const physicalTableName = instruction.physicalTableName;
      const destinationName = instruction.destination!;
      const sourceConnection = environment.getApiConnection(
         persistSource.connectionName,
      );
      // Resolved from the destination list, never the connection list: a build
      // naming a destination that is not configured has to fail here rather than
      // fall through to a same-named connection and write a tenant's own
      // warehouse. The throw surfaces as a 422 on the run.
      const destinationConnection =
         environment.getStorageDestination(destinationName);

      // Incremental refresh of a STORED table. The delta's rows are computed by the
      // source warehouse and its DML runs in the destination engine, which is what
      // the target below expresses; everything about deciding seed-vs-delta is the
      // shared planner's, exactly as for a colocated table.
      const lineage =
         incremental && params.contentSourceEntityId
            ? incrementalLineage({
                 declaration: incremental.declarations[persistSource.sourceID],
                 dialect: persistSource.dialectName,
                 targetDialect: STORAGE_TARGET_DIALECT,
                 physicalTableName,
                 connectionName: persistSource.connectionName,
                 storageDestinationName: destinationName,
                 sourceEntityId: params.contentSourceEntityId,
              })
            : undefined;
      // A CHAINED stored source is built by recompiling over its parents' lake
      // tables, and a delta over that is not designed yet: if the parent is itself
      // incremental, its own delta can restate rows BELOW this source's frontier,
      // which a half-open range would never revisit. So it rebuilds — reported
      // under its own reason code rather than left to look like a source that was
      // never incremental, which is what an absent `refresh` field would say.
      const chainedSeed = lineage !== undefined && dependsOnStorageUpstream;
      if (chainedSeed && lineage) {
         reportIncrementalStep({
            step: {
               mode: "seed",
               reasonCode: "chained_storage",
               reason:
                  `the source reads a storage-materialized upstream, and a delta ` +
                  `over a stored parent is not supported yet`,
            },
            sourceName: persistSource.name,
            packageName: incremental!.packageName,
            physicalTableName,
         });
         // Same reason the delta path clears a boundary before a rebuild: the
         // build below replaces the table this one describes, and a crash in
         // between must not leave a delta reading it. Reachable when a source that
         // was NOT chained becomes chained, which keeps both its name and its
         // content address.
         await resetLedger(incremental!, lineage);
      }
      const refresh: StorageIncrementalRefresh | undefined =
         incremental && lineage && !chainedSeed
            ? this.storageRefreshFor({
                 context: incremental,
                 lineage,
                 persistSource,
                 instruction,
                 destinationName,
                 physicalTableName,
                 buildSQL: params.buildSQL,
                 queryMetadata,
              })
            : undefined;

      const startTime = performance.now();
      let result;

      // Stack on the parent: a source that reads a storage-materialized
      // upstream is built by reading the parent's STORED lake table instead of
      // recomputing it from raw against the warehouse. This reuses the
      // parent's work and is consistent-by-construction (the downstream is a pure
      // function of the parent's stored rows). Attempted only when the source
      // actually reads a storage upstream; on any ineligibility (a parent
      // refinement not carried into the rebind, a live-warehouse join, a
      // cross-catalog parent) it throws and we fall back: recompute-from-raw when
      // non-strict, or a loud refusal under `strictUpstreams` (the orchestrated
      // contract — never silently recompute).
      if (dependsOnStorageUpstream) {
         try {
            result = await this.buildDownstreamViaParents(
               persistSource,
               destinationName,
               destinationConnection,
               builtEntries,
               environment,
               physicalTableName,
            );
            recordChainedStorageBuild("parent_reuse");
         } catch (err) {
            // Same redaction contract as the recompute-from-raw path below: this
            // branch's read-write ATTACH or CTAS can fail with the offending SQL
            // echoed back, catalog `password=` included. Redact before the
            // message reaches the thrown run `error` or the log.
            const safeDetail = redactConnectionSecrets(
               errMessage(err),
               sourceConnection,
               destinationConnection,
            );
            // Only a SHAPE failure justifies recomputing from raw: the downstream
            // could not be expressed over its rebound parents, so building it the
            // other way is a genuinely different attempt. An INFRA failure (the
            // read-write attach, the CTAS, the destination being down) is not —
            // recompute-from-raw writes to the SAME destination and fails the same
            // way, and metering it as `inline_fallback` files an outage in the same
            // bucket as a legitimate shape miss.
            if (!(err instanceof MaterializationEligibilityError)) {
               recordChainedStorageBuild("infra_failure");
               recordStorageBuildFailure(destinationName);
               throw new Error(
                  `Failed to materialize chained source '${persistSource.name}' ` +
                     `into storage destination '${destinationName}': ${safeDetail}`,
               );
            }
            if (manifest.strict) {
               recordChainedStorageBuild("strict_refused");
               recordStorageBuildFailure(destinationName);
               throw new Error(
                  `Failed to materialize chained source '${persistSource.name}' ` +
                     `into storage destination '${destinationName}' by reading ` +
                     `its materialized upstream, and strict upstreams forbid ` +
                     `recomputing it from raw: ${safeDetail}`,
               );
            }
            recordChainedStorageBuild("inline_fallback");
            logger.warn(
               "Chained storage build could not reuse the parent table; " +
                  "recomputing the upstream from raw",
               {
                  sourceName: persistSource.name,
                  destinationName,
                  reason: safeDetail,
               },
            );
         }
      }

      // Recompute from raw (the single-source passthrough): materialize `buildSQL`
      // (with storage upstreams inlined) in the source warehouse and CTAS the result
      // into the destination. Skipped when stacking on the parent already produced
      // the table above.
      if (!result) {
         try {
            result = await buildSourceIntoStorage({
               destinationName,
               destinationConnection,
               sourceConnection,
               buildSQL: params.publicBuildSQL,
               physicalTableName,
               environmentPath: environment.getEnvironmentPath(),
               queryMetadata,
               incremental: refresh,
            });
         } catch (err) {
            // Redaction: a failed federation / passthrough / attach
            // error can echo source- or catalog-connection detail (connstrings,
            // account names, service-account JSON) from the DuckDB engine. Strip
            // the actual credential VALUES but keep the message, so an operator
            // sees a legible, actionable error (e.g. "schema 'analytics' not
            // found" when a `name=schema.table` target's schema wasn't
            // provisioned) rather than an opaque failure — without leaking
            // secrets into the user-visible run `error` column.
            const safeDetail = redactConnectionSecrets(
               errMessage(err),
               sourceConnection,
               destinationConnection,
            );
            // Whether the warehouse read was already BILLED survives the
            // redaction, because it changes what a caller should do next. This
            // service's own scheduler advances "regardless of outcome so a
            // persistent failure retries on the next cron occurrence" and fires
            // with forceRefresh, which defeats skip-if-unchanged — so a
            // persistent cause re-runs the same read every occurrence, and on
            // this branch that means paying for it every occurrence. Rewrapping
            // as a bare Error made that indistinguishable from an attach
            // failure, which is free to retry.
            const alreadyBilled = err instanceof BilledReadNotCapturedError;
            recordStorageBuildFailure(
               destinationName,
               alreadyBilled ? "billed_read_not_captured" : "build_failed",
            );
            logger.warn("Storage materialization build failed", {
               sourceName: persistSource.name,
               destinationName,
               alreadyBilled,
               error: safeDetail,
            });
            const failure =
               `Failed to materialize source '${persistSource.name}' into ` +
               `storage destination '${destinationName}': ${safeDetail}`;
            throw alreadyBilled
               ? new BilledReadNotCapturedError(failure, { cause: err })
               : new Error(failure, { cause: err });
         }
      }

      // Build-time servability gate: the serve-shape must compile in DuckDB
      // against the authoritative post-build schema, or the build is refused
      // (HTTP 422) — a serve-time execution error turned into a fail-loud
      // build-time refusal. Outside the redaction try above so the eligibility
      // error surfaces as-is (it carries no connection secrets). Runs here, in
      // stage→validate, because it needs the captured schema.
      try {
         await assertStorageServeShapeCompiles({
            destinationName,
            sourceName: persistSource.name,
            virtualHandle: sourceEntityId,
            physicalTableName,
            schema: result.schema,
         });
      } catch (gateErr) {
         // An in-place refresh is exempt from the drop below, and that exemption
         // is the whole point of the branch: the table it advanced is the LIVE
         // serving generation, so dropping it would take a serving table out over
         // a shape the delta could not have changed (a definitional change
         // re-addresses the source, and a shape that drifted anyway forces a
         // rebuild). The refusal still fails the run, which leaves the boundary
         // unrecorded by the caller and the next refresh re-applying the same
         // idempotent range.
         if (result.refresh) throw gateErr;
         // The table was already CTAS'd before this post-build gate, and no
         // manifest entry records it yet — so a refusal would strand it where
         // manifest-driven GC (which only drops names it recorded building) can
         // never see it. Best-effort drop of the just-built table so a refused
         // build doesn't leak an orphaned table.
         //
         // Note (in-place naming): the CTAS above already replaced any prior
         // generation at this name, so a failed-gate rebuild has no earlier
         // table to fall back to — the source reverts to serving live until a
         // subsequent successful build. Rollback-safe regeneration (keep the
         // prior generation until the new one is validated) requires the
         // host-generational orchestrated path, not the auto-run server.
         try {
            await dropStorageTable({
               destinationName,
               destinationConnection,
               physicalTableName,
               environmentPath: environment.getEnvironmentPath(),
            });
         } catch (dropErr) {
            logger.warn(
               "Failed to drop a storage table stranded by a serve-shape gate " +
                  "refusal (physical leak)",
               {
                  sourceName: persistSource.name,
                  destinationName,
                  physicalTableName,
                  // The drop runs on a read-write attach, so a failure can echo
                  // the catalog connstring.
                  error: redactConnectionSecrets(
                     errMessage(dropErr),
                     sourceConnection,
                     destinationConnection,
                  ),
               },
            );
         }
         throw gateErr;
      }

      // Make this table visible to downstream sources built later in this run,
      // so a chained storage source can stack on it (above).
      manifest.update(sourceEntityId, { tableName: physicalTableName });

      const durationMs = Math.round(performance.now() - startTime);
      // A delta and a rebuild have different cost profiles, so they get different
      // series — and a SKIP is timed by neither, having done no work at all.
      if (result.refresh?.refresh === "delta") {
         recordSourceBuildDuration(durationMs, "delta_storage");
      } else if (!result.refresh) {
         recordSourceBuildDuration(durationMs, "storage");
      }
      logger.info(
         result.refresh
            ? `Refreshed materialized source ${persistSource.name} in storage`
            : `Built materialized source ${persistSource.name} into storage`,
         {
            physicalTableName,
            storageDestinationName: result.storageDestinationName,
            columns: result.schema.length,
            durationMs,
            refresh: result.refresh?.refresh,
            // The whole cost, not just the one field the manifest carries. These
            // are the numbers that answer a cost question and the ids that let a
            // human reach the job in the warehouse's own console — the manifest
            // has room for neither. Null says the read's shape reported nothing,
            // which is not the same as free: see BuildReadCost.
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
         rowCount: null,
         // A refresh reports what the APPLY took, matching the colocated path and
         // the field's own definition ("around the build itself"), rather than the
         // session setup around it. A SKIP applied nothing and so reports null:
         // the wall-clock of deciding that would average into the series as an
         // implausibly fast build.
         buildDurationMs: result.refresh
            ? (result.refresh.durationMs ?? null)
            : durationMs,
         // Where the table's coverage now reaches, and what this run DID to it —
         // reported for a stored table exactly as for a colocated one, so a caller
         // reads incremental progress rather than inferring it. Absent for a
         // source that is not refreshed incrementally.
         ...ledgerFields(
            lineage,
            result.refresh?.coveredThrough ?? result.seededThrough,
         ),
         ...refreshFields(
            result.refresh?.refresh ?? (lineage ? "full" : undefined),
         ),
         // SCANNED, matching the colocated path above, which fills this from the
         // connector's runStats -- and that is totalBytesProcessed, i.e. scanned.
         // Reporting billed here would put two different quantities in one field,
         // differing by up to BigQuery's 10MB floor, and anyone summing it across
         // a package's sources would add them together.
         //
         // Null when the read's shape reported nothing: a rows-returning
         // passthrough call hands back no job to account for. Today that means a
         // build whose metadata bag was empty, since it is the label that makes
         // BigQuery's read a form that reports.
         queryCostBytes: result.readCost?.bytesScanned ?? null,
      };
   }

   /**
    * The in-place refresh a stored source may get instead of a rebuild, as the two
    * moments a build session can offer it: before the CTAS (advance, or decline
    * and let the rebuild run) and after one (record where the rebuilt table
    * reaches).
    *
    * Both halves have to run INSIDE the session, which is why they are callbacks
    * rather than steps around the build: the plan probes the destination to decide,
    * and the post-rebuild boundary is probed from the table itself — and the
    * session holding that destination read-write exists only for this one source's
    * refresh.
    */
   private storageRefreshFor(params: {
      context: IncrementalRunContext;
      lineage: IncrementalLineage;
      persistSource: PersistSource;
      instruction: BuildInstruction;
      destinationName: string;
      physicalTableName: string;
      /** The unprojected build SQL — see storageDeltaTarget. */
      buildSQL: string;
      queryMetadata?: QueryMetadata;
   }): StorageIncrementalRefresh {
      const { context, lineage, persistSource, instruction } = params;
      return {
         plan: async ({ session, sourceType, handle, quotedTablePath }) => {
            const { target, readCost } = storageDeltaTarget({
               session,
               sourceType,
               handle,
               destinationName: params.destinationName,
               physicalTableName: params.physicalTableName,
               quotedTablePath,
               lineage,
               persistSource,
               buildSQL: params.buildSQL,
               queryMetadata: params.queryMetadata,
            });
            const step = await planSourceRefresh({
               context,
               lineage,
               target,
               columns: deriveColumns(persistSource).map((c) => String(c.name)),
               reseed: instruction.reseed,
            });
            const outcome = await applyIncrementalStep({
               context,
               lineage,
               step,
               target,
               sourceName: persistSource.name,
            });
            if (!outcome.applied) {
               // The rebuild about to run replaces the table the boundary
               // describes, so the boundary is dropped BEFORE it starts rather
               // than overwritten after: a crash mid-rebuild would otherwise leave
               // a boundary pointing at data that no longer exists, and the next
               // run would apply a delta on top of it.
               await resetLedger(context, lineage);
               return undefined;
            }
            return {
               durationMs: outcome.durationMs,
               coveredThrough: outcome.coveredThrough,
               refresh: outcome.refresh ?? "none",
               readCost: readCost(),
            };
         },
         afterSeed: ({ session, quotedTablePath }) =>
            advanceLedgerAfterSeed({
               context,
               lineage,
               quotedTablePath,
               dialect: STORAGE_TARGET_DIALECT,
               runner: (sql) => session.runSQL(sql),
            }),
      };
   }

   /**
    * Stack on the parent: materialize a chained storage source by reading its
    * already-materialized upstream(s) from the SAME destination store. Rebinds
    * every same-destination materialized upstream to a virtual source (base-only,
    * the captured schema), re-declares the downstream over them (its definition
    * text lifted from the author's model), and hands the assembled transient
    * model to {@link buildDownstreamIntoStorage}, which compiles it against the
    * build session and CTASes the downstream's SQL — now reading the parents'
    * lake tables — into the destination.
    *
    * Throws (⇒ the caller falls back to recompute-from-raw) when there is no
    * same-destination upstream to build on, the definition text can't be lifted,
    * or the transient model doesn't compile (a parent refinement not carried, a
    * live-warehouse join, a cross-catalog parent).
    */
   private async buildDownstreamViaParents(
      persistSource: PersistSource,
      destinationName: string,
      destinationConnection: ApiConnection,
      builtEntries: Record<string, ManifestEntry>,
      environment: BuildEnvironment,
      physicalTableName: string,
   ): Promise<StorageBuildResult> {
      // Rebind every upstream materialized into THIS destination. A parent in a
      // DIFFERENT destination is absent here, so the downstream def fails to
      // compile against the rebind model and the caller falls back — cross-catalog
      // parent reuse is out of scope for the spike.
      const upstreams: ServeBinding[] = deriveServeBindings(
         builtEntries,
      ).filter((b) => b.destinationName === destinationName);
      if (upstreams.length === 0) {
         throw new MaterializationEligibilityError({
            message:
               "no materialized upstream is available in this destination to build on",
         });
      }
      const downstreamDefText = this.liftDownstreamDefText(persistSource);
      if (!downstreamDefText) {
         throw new MaterializationEligibilityError({
            message:
               "could not recover the downstream source definition text from the model",
         });
      }
      const transientModel = buildChainedStorageBuildModel({
         upstreams,
         downstreamName: persistSource.name,
         downstreamDefText,
         destinationName,
      });
      return buildDownstreamIntoStorage({
         destinationName,
         destinationConnection,
         transientModel,
         downstreamName: persistSource.name,
         virtualMap: buildVirtualMap(upstreams),
         physicalTableName,
         environmentPath: environment.getEnvironmentPath(),
      });
   }

   /**
    * Lift the verbatim RHS of a persist source's `source: <name> is …`
    * declaration from the author's model file (same technique as the serve
    * transform's join/view lift): read the file named by the source's compiled
    * `location`, slice the covered range. A top-level source's range starts just
    * after the `source: ` keyword, so the result is `<name> is <def>` and the
    * transient-model assembler prepends `source: `. Returns undefined (⇒ fall
    * back) when there is no file-backed location or the file can't be read/sliced.
    */
   private liftDownstreamDefText(
      persistSource: PersistSource,
   ): string | undefined {
      const location = (
         persistSource._explore as unknown as { location?: SourceLocation }
      ).location;
      if (!location?.url?.startsWith("file:")) return undefined;
      let text: string;
      try {
         text = readFileSync(fileURLToPath(location.url), "utf8");
      } catch {
         return undefined;
      }
      return sliceSourceRange(text, location.range);
   }

   // ==================== CANCELLATION ====================

   /** Cancel a non-terminal materialization. */
   async stopMaterialization(
      environmentName: string,
      packageName: string,
      id: string,
   ): Promise<Materialization> {
      const m = await this.getMaterialization(environmentName, packageName, id);

      const cancellable: MaterializationStatus[] = [
         "PENDING",
         "MANIFEST_ROWS_READY",
      ];
      if (!cancellable.includes(m.status)) {
         throw new InvalidStateTransitionError(
            `Materialization ${id} is ${m.status}, cannot stop`,
         );
      }

      const abortController = this.runningAbortControllers.get(id);
      if (abortController) {
         abortController.abort();
         return m;
      }
      return this.transition(id, "CANCELLED", {
         completedAt: new Date(),
         error: "Cancelled",
      });
   }

   /**
    * Delete a materialization record. Only terminal materializations
    * (MANIFEST_FILE_READY, FAILED, CANCELLED) can be deleted; an active run must
    * be stopped first.
    *
    * By default this removes the publisher's record only — physical-table GC is
    * the caller's responsibility. When `dropTables` is set, the publisher
    * additionally drops the physical tables recorded in this run's manifest as a
    * best-effort cleanup (a drop failure is logged, not fatal, so the record
    * still deletes).
    */
   async deleteMaterialization(
      environmentName: string,
      packageName: string,
      id: string,
      options: { dropTables?: boolean } = {},
   ): Promise<void> {
      const m = await this.getMaterialization(environmentName, packageName, id);

      const terminal: MaterializationStatus[] = [
         "MANIFEST_FILE_READY",
         "FAILED",
         "CANCELLED",
      ];
      if (!terminal.includes(m.status)) {
         throw new InvalidStateTransitionError(
            `Cannot delete materialization ${id} while it is ${m.status}`,
         );
      }

      if (options.dropTables) {
         await this.dropMaterializedTables(environmentName, packageName, m);
      }

      await this.repository.deleteMaterialization(id);

      // Re-derive the package's serve routing from the latest REMAINING
      // successful materialization — BOTH tiers. The deleted run may have been
      // the one bound for serving — and with `dropTables` its tables are now gone
      // — so without this a query would keep routing (schema-on-faith, no
      // run-time fallback) to a deleted/dropped table and error until the next
      // reload or build. Rebinding picks the next-latest generation, or CLEARS
      // the bindings when none remain (empty ⇒ serve live).
      await this.rebindServeBindingsAfterDelete(environmentName, packageName);
   }

   /**
    * Re-derive a package's serve routing from its latest remaining successful
    * materialization and push it onto the loaded models — BOTH tiers, mirroring
    * the load-time {@link Environment.rebindServeBindingsFromLocalStore}. Called
    * after a delete so serving never points at a removed table; picks the
    * next-latest generation, or clears the bindings when none remain.
    * Best-effort (a failure logs and leaves the current bindings — a later
    * reload/build re-derives).
    *
    * The manifest is split by tier:
    *  - **colocated** (same-connection) → re-derived regardless of
    *    `PERSIST_STORAGE_MODE`: colocated is the v0 path and is not gated by the
    *    storage kill switch, so a reclaimed colocated table must not be left
    *    routed even when the tier is off.
    *  - **storage=** (cross-connection) → re-derived only when the tier is not
    *    `off` (its serve routing requires the tier on; an off deployment does no
    *    extra work here).
    */
   private async rebindServeBindingsAfterDelete(
      environmentName: string,
      packageName: string,
   ): Promise<void> {
      try {
         const environmentId = await this.resolveEnvironmentId(environmentName);
         // "" excludes nothing — the deleted record is already gone from the repo.
         const entries = await this.getMostRecentManifestEntries(
            environmentId,
            packageName,
            "",
         );
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const { tableNameManifest, storageEntries } = splitManifestEntries(
            entries,
            `post-delete rebind (package ${packageName})`,
         );
         // Colocated: re-derive (or clear) regardless of mode.
         await environment.bindPackageColocatedServeManifest(
            packageName,
            tableNameManifest,
         );
         // Storage=: only meaningful when the tier is not off.
         if (getPersistStorageMode() !== "off") {
            await environment.bindPackageStorageServeBindings(
               packageName,
               storageEntries,
            );
         }
      } catch (err) {
         logger.warn(
            "Failed to rebind serve bindings after delete (leaving current " +
               "bindings; a reload/build will re-derive)",
            { packageName, error: errMessage(err) },
         );
      }
   }

   /**
    * Best-effort drop of every physical table this run produced, read from the
    * materialization's manifest. Resolves each entry's connection by name and
    * issues `DROP TABLE IF EXISTS` for the physical table and its (possible)
    * leftover staging table. Failures are logged and swallowed so a partial
    * cleanup never blocks deletion of the record.
    *
    * A physical name is dropped only when NO other remaining MANIFEST_FILE_READY
    * run references it. Physical names are the source's `name=` verbatim (or a
    * host-assigned name), so multiple generations of a source share one physical
    * name — dropping a superseded record's table would otherwise take out the
    * table the current generation still serves. The skip incidentally also
    * protects the colocated case, where generations likewise share a
    * name.
    */
   private async dropMaterializedTables(
      environmentName: string,
      packageName: string,
      m: Materialization,
   ): Promise<void> {
      const entries = m.manifest?.entries;
      if (!entries || Object.keys(entries).length === 0) {
         return;
      }

      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const pkg = await environment.getPackage(packageName, false);
      const connectionCache = new Map<string, MalloyConnection>();

      // Physical names still referenced by ANOTHER MANIFEST_FILE_READY run for
      // this package (keyed destination-and-name), so a shared name is never
      // dropped out from under a live generation. `m` is still in the repo at
      // this point (deletion happens after this sweep), so exclude it by id.
      const tableKey = (dest: string, table: string) => `${dest}:${table}`;
      const stillReferenced = new Set<string>();
      const environmentId = await this.resolveEnvironmentId(environmentName);
      const others =
         (await this.repository.listMaterializations(
            environmentId,
            packageName,
         )) ?? [];
      for (const other of others) {
         if (other.id === m.id) continue;
         if (other.status !== "MANIFEST_FILE_READY") continue;
         for (const e of Object.values(other.manifest?.entries ?? {})) {
            const dest = e.storageDestinationName ?? e.connectionName;
            if (dest && e.physicalTableName) {
               stillReferenced.add(tableKey(dest, e.physicalTableName));
            }
         }
      }

      for (const entry of Object.values(entries)) {
         const connectionName = entry.connectionName;
         const physicalTableName = entry.physicalTableName;
         if (!connectionName || !physicalTableName) {
            logger.warn("Skipping manifest entry with no connection/table", {
               materializationId: m.id,
               sourceEntityId: entry.sourceEntityId,
            });
            continue;
         }

         // Do not drop a table another live generation still serves (shared
         // physical name — see the method doc).
         const destForKey = entry.storageDestinationName ?? connectionName;
         if (stillReferenced.has(tableKey(destForKey, physicalTableName))) {
            logger.info(
               "Skipping drop: table still referenced by another materialization",
               {
                  materializationId: m.id,
                  physicalTableName,
                  destination: destForKey,
               },
            );
            continue;
         }

         // A storage= table lives in `storageDestinationName` (a DuckDB/DuckLake
         // destination), not in `connectionName` (the source warehouse), and
         // dropping it needs a build-scoped READ-WRITE attach — the serve attach
         // is read-only — so it is dropped on its own RW session rather than on
         // the (wrong-engine, read-only) source connection. Best-effort: a
         // failure is logged and the sweep continues, so one unreachable
         // destination never blocks reclaiming the rest.
         if (entry.storageDestinationName) {
            const destinationConnection = this.destinationForCleanup(
               environment,
               entry.storageDestinationName,
            );
            if (!destinationConnection) continue;
            try {
               await dropStorageTable({
                  destinationName: entry.storageDestinationName,
                  destinationConnection,
                  physicalTableName,
                  environmentPath: environment.getEnvironmentPath(),
               });
               recordDropTables("success", "storage");
               logger.info("Dropped materialized storage table on delete", {
                  materializationId: m.id,
                  physicalTableName,
                  storageDestinationName: entry.storageDestinationName,
               });
            } catch (err) {
               recordDropTables("failure", "storage");
               logger.warn("Failed to drop a storage-materialized table", {
                  materializationId: m.id,
                  physicalTableName,
                  storageDestinationName: entry.storageDestinationName,
                  // The drop attaches read-write, so a failure can echo the
                  // catalog connstring.
                  error: redactConnectionSecrets(
                     errMessage(err),
                     destinationConnection,
                  ),
               });
            }
            continue;
         }

         try {
            let connection = connectionCache.get(connectionName);
            if (!connection) {
               connection = await pkg.getMalloyConnection(connectionName);
               connectionCache.set(connectionName, connection);
            }
            // Dialect-quote from the live connection, the same way
            // buildOneSource quoted at build time, so a name that built
            // successfully also drops successfully (container paths, hyphenated
            // BigQuery project ids, etc.).
            const dialect = connection.dialectName;
            // Dropping a materialized table is warehouse work someone will have
            // to account for later, so it is tagged like the build that created
            // it — `ops` rather than `materialize`, because this is the
            // lifecycle operation, not a build.
            const dropOptions = this.dropRunSQLOptions(
               environment,
               connectionName,
               environmentName,
               packageName,
               m.id,
            );
            await connection.runSQL(
               `DROP TABLE IF EXISTS ${quoteTablePath(
                  physicalTableName,
                  dialect,
               )}`,
               dropOptions,
            );
            // A crash between staging-create and rename can leave the staging
            // table behind; clean it up too while we hold the connection.
            await connection.runSQL(
               `DROP TABLE IF EXISTS ${quoteTablePath(
                  `${physicalTableName}${stagingSuffix(entry.sourceEntityId)}`,
                  dialect,
               )}`,
               dropOptions,
            );
            recordDropTables("success", "in_warehouse");
            logger.info("Dropped materialized table on delete", {
               materializationId: m.id,
               physicalTableName,
               connectionName,
            });
         } catch (err) {
            recordDropTables("failure", "in_warehouse");
            logger.warn("Failed to drop materialized table on delete", {
               materializationId: m.id,
               physicalTableName,
               connectionName,
               error: errMessage(err),
            });
         }
      }
   }

   /**
    * Finalize a successful build: advance to MANIFEST_ROWS_READY then
    * MANIFEST_FILE_READY and persist the assembled manifest. The caller
    * supplies the per-run metadata. The build itself happens before this.
    */
   private async commitManifest(
      id: string,
      entries: Record<string, ManifestEntry>,
      failures: Record<string, SourceFailure>,
      metadata: Record<string, unknown>,
   ): Promise<void> {
      await this.transition(id, "MANIFEST_ROWS_READY");
      const manifest: BuildManifestResult = {
         builtAt: new Date().toISOString(),
         entries,
         // Omitted entirely on a clean run rather than written as {}: a manifest
         // that records no failures and one that records an empty set are the
         // same fact, and the absent key is the one every earlier manifest has.
         ...(Object.keys(failures).length > 0 ? { failures } : {}),
         strict: false,
      };
      await this.transition(id, "MANIFEST_FILE_READY", {
         completedAt: new Date(),
         manifest,
         metadata,
      });
   }

   // ==================== HELPERS ====================

   /** The single-active conflict error, with the winning run's id when known. */
   private activeConflict(
      packageName: string,
      activeId?: string,
   ): MaterializationConflictError {
      const suffix = activeId ? ` (${activeId})` : "";
      return new MaterializationConflictError(
         `Package ${packageName} already has an active materialization${suffix}`,
      );
   }

   private recordRun(
      mode: MaterializationMode,
      outcome: "success" | "partial" | "failed" | "cancelled",
      startedAtMs: number,
   ): void {
      recordMaterializationRun(mode, outcome, Date.now() - startedAtMs);
   }

   private runInBackground(
      id: string,
      run: (signal: AbortSignal) => Promise<void>,
   ): void {
      const abortController = new AbortController();
      this.runningAbortControllers.set(id, abortController);

      run(abortController.signal)
         .catch(async (err) => {
            // Per-source build failures arrive already redacted (see the "Source
            // failed to materialize" site). A whole-run throw carries no
            // connection to redact against, but it can still echo a DSN -- a
            // connect failure while resolving the package's connections does --
            // and redactPgSecrets needs none, so it is the floor for both the log
            // and the persisted record.
            const message = redactPgSecrets(errMessage(err));
            const cancelled = abortController.signal.aborted;
            const next = cancelled ? "CANCELLED" : "FAILED";
            // The materialization record carries the message to whoever polls
            // it, but a background run's throw answers no request -- so without
            // a log here the failure is invisible in the server's own output,
            // and the stack, the only thing that locates the throw, is dropped.
            if (cancelled) {
               // The error too: an abort that races a genuine failure records
               // "Cancelled", and this is then the only place the real one is
               // reported.
               logger.info("Materialization run cancelled", {
                  materializationId: id,
                  error: message,
               });
            } else {
               logger.error("Materialization run failed", {
                  materializationId: id,
                  error: message,
                  stack: err instanceof Error ? err.stack : undefined,
               });
            }
            try {
               await this.repository.updateMaterialization(id, {
                  status: next,
                  completedAt: new Date(),
                  error: cancelled ? "Cancelled" : message,
               });
            } catch (transitionErr) {
               logger.error("Failed to record materialization failure", {
                  materializationId: id,
                  originalError: message,
                  transitionError: errMessage(transitionErr),
               });
            }
         })
         .finally(() => {
            this.runningAbortControllers.delete(id);
         });
   }

   private resolveEnvironmentId(environmentName: string): Promise<string> {
      return resolveEnvironmentId(this.repository, environmentName);
   }
}
