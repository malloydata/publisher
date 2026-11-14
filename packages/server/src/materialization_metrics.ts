// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Centralized telemetry for materialization builds.
 *
 * Operators need to answer "are builds completing, how long do they take, and
 * where are they failing?" without grepping logs. The run counter carries
 * `mode` (`auto`|`orchestrated`) and `outcome` (`success`|`partial`|`failed`|`cancelled`);
 * the run-duration histogram carries `mode` so a dashboard can render auto-run
 * vs orchestrated build latency side by side.
 *
 * Instruments are created lazily for the same reason as {@link ./query_cap_metrics}:
 * one created before `setGlobalMeterProvider` binds to a NoOp meter
 * (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import {
   lazyCounter,
   lazyHistogram,
   resetInstrumentsForTesting,
} from "./telemetry";

/**
 * Every label below is a closed union whose values are listed in a const array
 * (materialization v2 plan, section 5, "Step 4 specification"), so a spec can
 * enumerate each one and adding a value is a reviewed change. No instrument
 * here carries a package, environment, source, model, table, destination,
 * connection or query label, and none is filled from a runtime string.
 */
export const MATERIALIZATION_MODES = ["auto", "orchestrated"] as const;
export type MaterializationMode = (typeof MATERIALIZATION_MODES)[number];

/**
 * What a build makes: `materialize` a `#(materialize)` table, `persist` a
 * `#@ persist` table, `rollup` a `#(preaggregate)` or `#@ preaggregate`
 * rollup, `view` a persisted view, `pair` a pair table (an inlined join_many).
 */
export const BUILD_KINDS = [
   "materialize",
   "persist",
   "rollup",
   "view",
   "pair",
] as const;
export type BuildKind = (typeof BUILD_KINDS)[number];

/** Where a table is built: the source's own warehouse, or a `storage=` destination. */
export const BUILD_TIERS = ["colocated", "storage"] as const;
export type BuildTier = (typeof BUILD_TIERS)[number];

/** A full rebuild, or an incremental delta applied in place. */
export const BUILD_METHODS = ["full", "delta"] as const;
export type BuildMethod = (typeof BUILD_METHODS)[number];

/**
 * One source's outcome in a run: `built` (a skipped incremental refresh
 * included, since its table stands current), `reused` (carried forward
 * unchanged, or supplied by reference), `refused` (the eligibility gate),
 * `failed`, or `cancelled` (failed because the run was cancelled).
 */
export const BUILD_OUTCOMES = [
   "built",
   "reused",
   "refused",
   "failed",
   "cancelled",
] as const;
type BuildOutcome = (typeof BUILD_OUTCOMES)[number];

/**
 * Why a build failed, for whoever decides on a retry: `deterministic`,
 * `transient` and `unclassified` are `BuildRetryable` false, true and
 * undefined (build_failure_class.ts); `billed_read_lost` is a `storage=` build
 * whose warehouse read ran and was charged and whose rows were not captured,
 * so a retry pays for it again. `none` is every outcome but `failed`.
 */
export const FAILURE_CLASSES = [
   "none",
   "deterministic",
   "transient",
   "unclassified",
   "billed_read_lost",
] as const;
export type FailureClass = (typeof FAILURE_CLASSES)[number];

/** Why an incremental refresh seeded or skipped (the step counter's `reason`). */
export const INCREMENTAL_STEP_REASONS = [
   // Seeds.
   "forced",
   "no_boundary",
   "lineage_changed",
   "table_renamed",
   "merge_unsupported",
   "table_unreadable",
   "table_emptied",
   "shape_mismatch",
   "ledger_unreadable",
   "plan_error",
   "chained_storage",
   "pair_parent_row",
   // Skips.
   "frontier_unreadable",
   "not_advanced",
] as const;
export type IncrementalStepReason = (typeof INCREMENTAL_STEP_REASONS)[number];

/** Bucket boundaries of the build-duration histogram, in milliseconds. */
export const BUILD_DURATION_BUCKETS_MS = [
   100, 500, 1_000, 5_000, 15_000, 60_000, 300_000, 900_000, 3_600_000,
];
/** Bucket boundaries of the build-read histogram, in bytes: 1 MB to 10 TB by decades. */
const BUILD_READ_BYTES_BUCKETS = [1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13];
/**
 * `partial` is a run that committed a manifest while some of its sources failed:
 * distinct from `success` because the manifest is missing tables a consumer
 * expected, and from `failed` because the sources that did build are usable.
 */
export const MATERIALIZATION_OUTCOMES = [
   "success",
   "partial",
   "failed",
   "cancelled",
] as const;
export type MaterializationOutcome = (typeof MATERIALIZATION_OUTCOMES)[number];
/** Manifest bind outcome: timeout is split out from generic failure on purpose. */
type ManifestBindOutcome = "success" | "failure" | "timeout";
/**
 * Which engine a table drop ran against (`publisher_materialization_drop_tables_total`):
 * the source's own warehouse (`in_warehouse`) or a DuckDB/DuckLake `storage=`
 * destination (`storage`). `delta` and `delta_storage` are kept on the union
 * for the label's history; a build's tier and method are labels of their own
 * on the build instruments ({@link BuildTier}, {@link BuildMethod}).
 */
type StorageBuildEngine =
   | "storage"
   | "in_warehouse"
   | "delta"
   | "delta_storage";
/** Why a source was refused materialization into a storage destination. */
export type EligibilityRefusalReason =
   | "free_parameter"
   | "given"
   | "given_in_persisted_query"
   | "dynamic_projection"
   | "dynamic_join"
   | "dynamic_joined_where"
   | "partition_without_storage"
   | "partition_column_unknown"
   | "partition_column_not_public"
   | "merge_key_scope_unresolved"
   | "preaggregate_over_dynamic_source"
   | "authorize"
   | "not_duckdb_portable"
   | "public_surface_unknown"
   | "read_only_dialect"
   | "env_reference"
   | "rollup_name_collision";
/**
 * How a chained `storage=` source (one that reads a storage-materialized
 * upstream) was built: `parent_reuse` = built by reading the upstream's stored
 * lake table ("stack on the parent" — reuses the parent's work and is
 * consistent-by-construction); `inline_fallback` = stacking was ineligible/failed
 * so the upstream was recomputed from raw against the warehouse (non-strict);
 * `strict_shape_fallback` = under `strictUpstreams`, the downstream reaches the
 * source warehouse (a table joined beside a stored parent, through a
 * non-persisted source), so no build over the parents exists and it was
 * recomputed from raw — the one recompute strict permits; `strict_refused` =
 * under `strictUpstreams`, a persisted upstream was neither built in the run
 * nor referenced, or the downstream reads only stored upstreams yet could not be
 * built over them, so the build failed loudly rather than recomputing a table
 * the orchestrator meant to pin.
 */
type ChainedStorageBuildOutcome =
   | "parent_reuse"
   | "inline_fallback"
   | "strict_shape_fallback"
   | "strict_refused"
   // The parent-reuse attempt failed on infrastructure (attach, CTAS, the
   // destination being unreachable) rather than on shape. Distinct from
   // `inline_fallback` because it is an outage, not a modelling limit: the build
   // fails rather than recomputing from raw against the same destination.
   | "infra_failure";

const runCounter = lazyCounter(
   "publisher_materialization_runs_total",
   "Materialization builds completed. Labels: mode ('auto'|'orchestrated'), outcome ('success'|'partial'|'failed'|'cancelled').",
);
const runDuration = lazyHistogram(
   "publisher_materialization_run_duration_ms",
   "Wall-clock duration of a materialization build. Label: mode ('auto'|'orchestrated').",
);
const buildCounter = lazyCounter(
   "publisher_materialization_build_total",
   "Source builds, one per source per run at its finish or failure. Labels: " +
      "kind ('materialize'|'persist'|'rollup'|'view'|'pair'), tier " +
      "('colocated'|'storage'), outcome ('built'|'reused'|'refused'|'failed'|" +
      "'cancelled'), failure_class ('none' unless outcome is 'failed'; then " +
      "'deterministic'|'transient'|'unclassified'|'billed_read_lost'). " +
      "'billed_read_lost' is the expensive one: the warehouse read ran and was " +
      "charged and its rows were not captured, so a retry pays for it again.",
);
const buildDuration = lazyHistogram(
   "publisher_materialization_build_duration_ms",
   "Wall-clock duration of one source's build, for builds that ran (built or " +
      "failed; a skipped incremental refresh did no work and is not timed). " +
      "Labels: kind, tier, method ('full'|'delta').",
   { buckets: BUILD_DURATION_BUCKETS_MS },
);
const buildReadBytes = lazyHistogram(
   "publisher_materialization_build_read_bytes",
   "Warehouse bytes scanned by one source's build read, when the warehouse " +
      "reported a count; absent otherwise, never zero. Label: tier.",
   { unit: "By", buckets: BUILD_READ_BYTES_BUCKETS },
);
const annotationFindingsCounter = lazyCounter(
   "publisher_annotation_findings_total",
   "Coded findings on authored annotations (the #@ route, and the near-miss and malformed-route scan over every route), counted once each time a package is loaded and accepted for serving (boot, add, publish, reload; a refused publish and a gate re-run count nothing). Labels: code (e.g. 'persist-unknown-key', 'persist-unparseable', 'annotation-near-miss'), tier ('malformed' for an error-severity finding, 'unservable' for a warning).",
);
const incrementalStepCounter = lazyCounter(
   "publisher_materialization_incremental_step_total",
   'Refreshes of a source declared refresh="incremental". Labels: step ' +
      "('delta'|'seed'|'skip'), and for seed/skip a bounded reason code " +
      "(IncrementalStepReasonCode). A 'seed' is a full rebuild the delta path " +
      "declined, so a rising seed rate means the feature is not engaging — and " +
      "the reason label says why without a log dive.",
);
const buildPlanComputeDuration = lazyHistogram(
   "publisher_materialization_build_plan_compute_duration_ms",
   "Wall-clock duration of compiling a package's build plan (Package.buildPlan).",
);
const buildPlanComputeFailedCounter = lazyCounter(
   "publisher_materialization_build_plan_compute_failed_total",
   "Package loads whose build plan failed to compute. The ROOT-CAUSE signal for " +
      "colocated_bind_dropped{reason='build_plan_unavailable'}, which fires only " +
      "per dropped entry per load -- a package that loads once and is never " +
      "reloaded ticks that counter once and then sits with its colocated tier " +
      "off and a flat total, indistinguishable from healthy.",
);
const autoLoadCounter = lazyCounter(
   "publisher_materialization_auto_load_total",
   "Auto-run manifest auto-load attempts. Label: outcome ('success'|'failure').",
);
const connectionDigestSkipCounter = lazyCounter(
   "publisher_materialization_connection_digest_skipped_total",
   "Connection digests skipped during build-plan compile because the connection did not resolve.",
);
const manifestBindCounter = lazyCounter(
   "publisher_materialization_manifest_bind_total",
   "Manifest bind attempts. Label: outcome ('success'|'failure'|'timeout').",
);
const manifestBindDegradedCounter = lazyCounter(
   "publisher_materialization_manifest_bind_degraded_total",
   "Manifest entries bound with an UNQUOTED table path because their connection " +
      "could not be resolved (serve-side bind) or is absent from the build " +
      "(build-side seed). A misconfiguration that breaks the source on a " +
      "case-folding engine (Snowflake); alertable.",
);
const duplicateTargetSkipCounter = lazyCounter(
   "publisher_materialization_duplicate_target_skipped_total",
   "Sources skipped because the physical table they name was already built in " +
      "this run. Ordinary for a package that extends a persisted source; a " +
      "rising count against a package with no extension means the plan is " +
      "enumerating one table under more names than expected.",
);
const sharedAddressInstructionCounter = lazyCounter(
   "publisher_materialization_shared_address_instructions_total",
   "Content addresses that arrived with more than one instruction naming a " +
      "DIFFERENT physical table. The host minted a table per source where " +
      "several sources share one artifact. With a sourceID on each instruction " +
      "every table is built and only one is recorded, so the rest are orphaned; " +
      "without one the last instruction wins and the earlier names are never " +
      "built. Wasteful, not wrong — the table's CONTENT is the same either way.",
);
const tableCollisionCounter = lazyCounter(
   "publisher_materialization_table_collision_total",
   "Two definitions with DIFFERENT content addresses materializing into ONE " +
      "physical table. Each build overwrites the other's rows while both " +
      "addresses resolve to the table at serve time, so a query is answered " +
      "from another source's data. A wrong answer, not wasted work — page on " +
      "this one. Counted, then refused before any write, unless " +
      "PERSIST_COLLISION_ENFORCE=false opts out, in which case the build " +
      "continues.",
);
const dropTablesCounter = lazyCounter(
   "publisher_materialization_drop_tables_total",
   "Physical tables dropped on delete. Label: outcome ('success'|'failure').",
);
const scheduledFireCounter = lazyCounter(
   "publisher_materialization_scheduled_fires_total",
   "Standalone-scheduler attempts to fire a package's materialization.schedule. " +
      "Label: outcome ('fired'|'conflict'|'error').",
);
const storageServeRoutingCounter = lazyCounter(
   "publisher_storage_serve_routing_total",
   "storage= serve routing decisions. Label: outcome ('storage'|'live_fallback'|" +
      "'runtime_live_fallback'|'blocked_by_row_level_gate'). Covers the storage= " +
      "tier only; a colocated #@ persist hit is in neither the numerator nor the " +
      "denominator. NOTE 'live_fallback' here means the transform was INELIGIBLE, " +
      "which QueryResult.servedFrom reports as null - that field's " +
      "'live_fallback' is this counter's 'runtime_live_fallback'.",
);
const queryVariantRoutingCounter = lazyCounter(
   "publisher_query_variant_routing_total",
   "The variant router's decision on each candidate it considered for a query, " +
      "one count per candidate (materialization v2, #6272). Labels: variant " +
      "('storage'|'rollup'), outcome ('used'|'skipped'), reason ('none' when used; " +
      "'row_level_gate'|'not_applicable'|'does_not_answer' when skipped, and " +
      "'run_failed' for a chosen variant whose run failed and fell back live). " +
      "Counted once the query is answered. A query with no candidate records nothing.",
);
const storageTableRetainedCounter = lazyCounter(
   "publisher_storage_tables_retained_total",
   "Tables a FAILED run left in a storage= destination and deliberately did not " +
      "reclaim, because the source is refreshed incrementally and the name may be " +
      "the one it serves from. Not all of these are orphans — " +
      "a rebuild at a fresh generational name is, a seed on the live serving name " +
      "is not, and the manifest entry cannot separate them — so read this as an " +
      "upper bound on what is accumulating rather than a leak count. It is the " +
      "only accounting there is until reclaiming a destination exists, which is " +
      "why it is a counter and not just a log line: the question is a rate, not " +
      "whether it ever happened. Unlabelled: the destination is on the warn " +
      "log that accompanies each count.",
);
const attributionSkippedCounter = lazyCounter(
   "publisher_storage_build_attribution_skipped_total",
   "storage= builds whose warehouse read went out UNATTRIBUTED while tagging was " +
      "on. Label: reason ('job_listing_unavailable'|'tag_failed'|" +
      "'read_row_not_found'|'read_row_ambiguous'|'cost_query_failed'). " +
      "The read still ran and the " +
      "build still succeeded — what was lost is the label in the customer's own " +
      "query history, and the cost on this side. Without this an operator who " +
      "turns tagging on and sees nothing has a single log line to go on.",
);
const eligibilityRefusedCounter = lazyCounter(
   "publisher_materialization_eligibility_refused_total",
   "materialization-eligibility refusals, both tiers. Label: reason " +
      "('free_parameter'|'given_in_persisted_query'|'dynamic_projection'|" +
      "'dynamic_join'|'dynamic_joined_where'|'partition_without_storage'|" +
      "'partition_column_unknown'|'partition_column_not_public'|" +
      "'merge_key_scope_unresolved'|'preaggregate_over_dynamic_source'|" +
      "'authorize'|'partition'|'not_duckdb_portable'|" +
      "'public_surface_unknown'|'read_only_dialect'). 'given' is retained on the enum for records " +
      "that carry it and is no longer produced: where a given sits decides the " +
      "outcome, so one reason covering every placement cannot be raised.",
);
const serveShapeTypeFallbackCounter = lazyCounter(
   "publisher_storage_serve_shape_type_fallback_total",
   "Captured DuckDB column types mapped to json in a stored table's declared " +
      "type (type fidelity loss). Named for the retired storage serve shape. " +
      "Label: kind ('array'|'unrecognized').",
);
const storedColumnTypeMismatchCounter = lazyCounter(
   "publisher_storage_column_type_mismatch_total",
   "Stored columns whose DuckDB type does not serve as the type Malloy " +
      "declares for them (ms2data/service#6642). Labels: stage ('build' = " +
      "found after a storage build; 'serve' = found when a model's bindings " +
      "are set, then cast on read or left out of the served copy so queries " +
      "reading it are answered live), declared (the Malloy type).",
);
const chainedStorageBuildCounter = lazyCounter(
   "publisher_storage_chained_build_total",
   "Chained storage= source builds (a source reading a storage-materialized " +
      "upstream). Label: outcome ('parent_reuse'|'inline_fallback'|" +
      "'strict_shape_fallback'|'strict_refused'|'infra_failure'). The " +
      "parent_reuse share is the headline signal for how far the " +
      "stack-on-the-parent path gets us vs recompute-from-raw.",
);
const staleUpstreamEntryCounter = lazyCounter(
   "publisher_materialization_stale_upstream_entry_total",
   "Builds that could not use an upstream's manifest entry because it is at a " +
      "different tier than the upstream is declared on now (ms2data/service#7087: " +
      "a source moved to or from storage= keeps its content address). The build " +
      "recomputes that upstream from raw instead, which reads the warehouse. " +
      "Label: build ('colocated'|'storage'), the tier of the build that " +
      "recomputed.",
);
const colocatedBindDroppedCounter = lazyCounter(
   "publisher_materialization_colocated_bind_dropped_total",
   "Colocated serve-manifest entries dropped by bindColocatedServeManifest. " +
      "Label: reason ('build_plan_unavailable' when the package's build plan " +
      "failed to compute, so no source was examined at all; 'refused' when the " +
      "source itself was examined and found ineligible). 'build_plan_unavailable' " +
      "is a whole-package regression -- colocated is the default tier and is NOT " +
      "gated by PERSIST_STORAGE_MODE, so every colocated binding for the package " +
      "reverts to live recompute until a load succeeds.",
);

/**
 * Record a standalone-scheduler fire attempt. `fired` = a SCHEDULER run started;
 * `conflict` = a materialization was already active (this occurrence coalesces
 * into it); `error` = the fire failed unexpectedly.
 */
export function recordScheduledFire(
   outcome: "fired" | "conflict" | "error",
): void {
   scheduledFireCounter().add(1, { outcome });
}

/** Record the outcome and duration of a materialization build. */
export function recordMaterializationRun(
   mode: MaterializationMode,
   outcome: MaterializationOutcome,
   durationMs: number,
): void {
   runCounter().add(1, { mode, outcome });
   runDuration().record(durationMs, { mode });
}

/**
 * One source's build outcome in a run. `failureClass` is required with
 * `failed` and absent otherwise, so the counter's `failure_class` is `none`
 * exactly when the outcome is not `failed`.
 */
type BuildOutcomeLabels =
   | {
        kind: BuildKind;
        tier: BuildTier;
        outcome: "failed";
        failureClass: Exclude<FailureClass, "none">;
     }
   | {
        kind: BuildKind;
        tier: BuildTier;
        outcome: Exclude<BuildOutcome, "failed">;
        failureClass?: never;
     };

/**
 * Count one source's build outcome. Called once per source per run, at the
 * build loop (instructed_build.ts) for a source the run tried, and at the
 * commit for one it reused; never from inside a tier's strategies, whose
 * retries and fallbacks would count one source twice.
 *
 * `refused` with an orchestrated run should stay rare: a host that builds
 * from the build plan never instructs a source the plan refused, so a count
 * there means the plan and the build's own gate disagreed about a source.
 */
export function recordBuildOutcome(labels: BuildOutcomeLabels): void {
   buildCounter().add(1, {
      kind: labels.kind,
      tier: labels.tier,
      outcome: labels.outcome,
      failure_class: labels.outcome === "failed" ? labels.failureClass : "none",
   });
}

/**
 * Record how long one source's build ran. Labelled by kind, tier and method:
 * a delta and a CTAS, or a passthrough into a destination and an in-warehouse
 * build, have latency profiles too different to pool.
 */
export function recordBuildDuration(
   durationMs: number,
   labels: { kind: BuildKind; tier: BuildTier; method: BuildMethod },
): void {
   buildDuration().record(durationMs, {
      kind: labels.kind,
      tier: labels.tier,
      method: labels.method,
   });
}

/**
 * Record the warehouse bytes one build read, when the warehouse reported a
 * count. Nothing is recorded for a missing or zero count: null is not zero,
 * and a zero would average into the series as a free read.
 */
export function recordBuildReadBytes(
   bytes: number | null | undefined,
   tier: BuildTier,
): void {
   if (typeof bytes !== "number" || !(bytes > 0)) return;
   buildReadBytes().record(bytes, { tier });
}

/**
 * Count the coded annotation findings of one package accepted for serving,
 * one increment per finding: `code` is the warning's code, `tier` its
 * severity (`error` is malformed, `warn` unservable). A `malformed` count can
 * only come from a package published before its refusal existed, since
 * publish refuses a malformed finding before the package is served.
 */
export function recordAnnotationFindings(
   warnings: ReadonlyArray<{ code?: string; severity?: string }>,
): void {
   for (const { code, severity } of warnings) {
      if (!code) continue;
      annotationFindingsCounter().add(1, {
         code,
         tier: severity === "error" ? "malformed" : "unservable",
      });
   }
}

/**
 * Record what one incremental source's refresh actually did. The delta:seed
 * ratio is the health signal for the feature: a source that declares incremental
 * refresh but keeps seeding is being rebuilt in full every run, which is correct
 * but costs exactly what the declaration was meant to save. The `reason` label
 * (seed/skip only) is the bounded code for why; the free-text specifics stay in
 * the accompanying warn log.
 */
export function recordIncrementalStep(
   step: "delta" | "seed" | "skip",
   reason?: IncrementalStepReason,
): void {
   incrementalStepCounter().add(1, reason ? { step, reason } : { step });
}

/**
 * Record the wall-clock cost of a package's build plan at load
 * (`Package.buildPlan`). `computed_in` separates the two populations: `worker`
 * is the package-load worker deriving it from the compile it already ran (see
 * WorkerBuildPlan.durationMs); `main_thread` is the derivation that compiles
 * the package again on the event loop, which a load takes only when the
 * worker left the plan at its soft deadline.
 */
export function recordBuildPlanComputeDuration(
   durationMs: number,
   computedIn: "worker" | "main_thread",
): void {
   buildPlanComputeDuration().record(durationMs, { computed_in: computedIn });
}

/**
 * Record that a package's build plan failed to compute at load. The package
 * name stays in the accompanying warn log rather than becoming a label, as
 * everywhere else in this file.
 */
export function recordBuildPlanComputeFailed(): void {
   buildPlanComputeFailedCounter().add(1);
}

/**
 * Record an auto-run manifest auto-load attempt. Auto-load is best-effort (a
 * failure does not fail the run), so a failure here is otherwise invisible:
 * the run is MANIFEST_FILE_READY but queries still resolve to the old tables.
 */
export function recordAutoLoadOutcome(outcome: "success" | "failure"): void {
   autoLoadCounter().add(1, { outcome });
}

/**
 * Record that a connection digest could not be computed during build-plan
 * compilation because the connection failed to resolve. The resulting sourceEntityIds
 * are computed without a digest, so this is a correctness signal, not just noise.
 */
export function recordConnectionDigestSkipped(): void {
   connectionDigestSkipCounter().add(1);
}

/**
 * Record a source skipped because its physical table was already built in this
 * run. Expected whenever several sources map onto one artifact — a base and its
 * `extend` share a content address — so this is a volume signal, not a fault.
 */
export function recordDuplicateTargetSkipped(): void {
   duplicateTargetSkipCounter().add(1);
}

/**
 * Record a content address that arrived with instructions naming more than one
 * physical table. The publisher cannot resolve this — the host asked for both
 * tables — so it builds each and records one, leaving the others unreferenced.
 */
export function recordSharedAddressInstructions(): void {
   sharedAddressInstructionCounter().add(1);
}

/**
 * Record two definitions materializing into one physical table. Distinct from
 * {@link recordSharedAddressInstructions}: that one is a host minting more tables
 * than an artifact needs (wasteful), this one is two different relations sharing a
 * table (serve-time wrong data).
 */
export function recordTableCollision(): void {
   tableCollisionCounter().add(1);
}

/**
 * Record a manifest-bind attempt (publisher binding a configured manifest to a
 * package at load). `timeout` is distinguished from `failure` so operators can
 * tell an unreachable/slow manifest store from a malformed manifest.
 */
export function recordManifestBind(outcome: ManifestBindOutcome): void {
   manifestBindCounter().add(1, { outcome });
}

/**
 * A single manifest entry bound with an unquoted table path because its
 * connection could not be resolved for quoting. Per-entry degradation (the rest
 * of the bind succeeds), distinct from the whole-bind {@link recordManifestBind}
 * outcome — hence its own counter rather than a label on that one.
 */
export function recordManifestBindDegraded(): void {
   manifestBindDegradedCounter().add(1);
}

/** Record a best-effort physical-table drop on materialization delete. */
export function recordDropTables(
   outcome: "success" | "failure",
   engine: StorageBuildEngine,
): void {
   dropTablesCounter().add(1, { outcome, engine });
}

/**
 * Record a stored table a failed run left behind without reclaiming. The
 * destination, which names a tenant's store, is on the accompanying log, not
 * a label.
 */
export function recordStorageTableRetained(): void {
   storageTableRetainedCounter().add(1);
}

/**
 * Record a build that ran its warehouse read WITHOUT attribution, despite tagging
 * being on. Not a failure — the build succeeded and the rows are correct — which
 * is exactly why it needs a counter: nothing else about the run looks wrong.
 */
export function recordAttributionSkipped(
   reason:
      | "job_listing_unavailable"
      | "tag_failed"
      | "read_row_not_found"
      | "read_row_ambiguous"
      | "cost_query_failed",
): void {
   attributionSkippedCounter().add(1, { reason });
}

/**
 * Record a materialization-eligibility refusal (a source that can't be safely
 * materialized into a storage destination). The `given` reason is a security
 * refusal (a frozen given-filtered table would leak rows across tenants); worth
 * tracking how often authors attempt it.
 */
export function recordEligibilityRefused(
   reason: EligibilityRefusalReason,
): void {
   const counts = eligibilityRefusalSink?.();
   if (counts) {
      counts[reason] = (counts[reason] ?? 0) + 1;
      return;
   }
   eligibilityRefusedCounter().add(1, { reason });
}

let eligibilityRefusalSink:
   | (() => Partial<Record<EligibilityRefusalReason, number>> | undefined)
   | undefined;

/**
 * Route eligibility refusals to the counts `sink` returns instead of the
 * meter, whenever it returns any. The package-load worker derives build plans
 * on a thread whose meter is a no-op, so it counts each job's refusals in that
 * job's own context and hands them back for the main thread to record.
 */
export function setEligibilityRefusalSink(
   sink: () => Partial<Record<EligibilityRefusalReason, number>> | undefined,
): void {
   eligibilityRefusalSink = sink;
}

/**
 * Record a captured DuckDB column type that could not map onto a Malloy basic
 * type and was carried as json in a stored table's declared type — a
 * type-fidelity loss specific to the storage tier. `array` = a collection/array type; `unrecognized`
 * = an unknown/nested/struct/enum/blob type.
 */
export function recordServeShapeTypeFallback(
   kind: "array" | "unrecognized",
): void {
   serveShapeTypeFallbackCounter().add(1, { kind });
}

/**
 * Record a stored column whose type does not serve as its declared Malloy type:
 * at `build`, found in the landed table's schema; at `serve`, found when a
 * model's bindings are set (the served copy casts it on read, or leaves it
 * out so queries reading it are answered live).
 */
export function recordStoredColumnTypeMismatch(
   stage: "build" | "serve",
   declared: string,
): void {
   storedColumnTypeMismatchCounter().add(1, { stage, declared });
}

/**
 * Record a `storage=` serve-routing decision, once per query, from the router's
 * decision: `storage` = the query was served from the materialized table via
 * the virtual-source transform; `live_fallback` = the transform was ineligible
 * for this query (a refinement it can't reproduce, a stale or unbound source),
 * so the next candidate (a colocated rollup) or the live warehouse answered;
 * `runtime_live_fallback` = the query DID route to storage and the store then
 * failed underneath it, and every binding's `freshnessFallback=live` allowed it
 * to degrade. That last one is the operationally interesting label: it means the
 * tier is broken while queries still succeed, which is invisible in the hit rate
 * alone. `blocked_by_row_level_gate` = a row-level-gated entry point vetoed
 * BOTH the storage and pre-aggregation tiers before either was attempted — the
 * one outcome with no compile attempt behind it, so without this label a
 * blocked query would record nothing. The fallback side also logs its reason
 * per query at INFO (the compile error, the sources the shape offered, and the
 * ones the freshness gate withheld), which separates "never materialized" from
 * "materialized but stale".
 *
 * ⚠️ `live_fallback` here is NOT `QueryResult.servedFrom`'s `live_fallback`, and
 * joining a dashboard across the two on that token is wrong in both directions.
 * This label means the transform was ineligible, which that field reports as
 * `null`; the run-time store failure that field calls `live_fallback` is
 * `runtime_live_fallback` here. (`manifestBindingStatus` spends the same token on
 * a third thing again — a configured `manifestLocation` whose fetch or bind
 * failed.) Each surface is internally consistent and none means what the others
 * do.
 *
 * This counter covers the `storage=` tier only: a COLOCATED `#@ persist` hit
 * never reaches the routing decision, so it is absent from numerator AND
 * denominator, not counted as a miss.
 *
 * `origin` splits storage hits: `persist` is an authored `storage=` source,
 * `preaggregate` a query answered by a lake-served rollup (both count as
 * storage hits in the total). Absent
 * when nothing routed. An attribute rather than a new `outcome` value because
 * consumers generate strict clients from the outcome enum, where an unexpected
 * value throws; an added attribute breaks no consumer.
 */
export function recordStorageServeRouting(
   outcome:
      | "storage"
      | "live_fallback"
      | "runtime_live_fallback"
      | "blocked_by_row_level_gate",
   origin?: "persist" | "preaggregate",
): void {
   storageServeRoutingCounter().add(1, {
      outcome,
      ...(origin ? { origin } : {}),
   });
}

/**
 * One candidate of a routed query: what the router did with it, and why.
 * `run_failed` is a chosen variant whose run failed and fell back live.
 */
export function recordQueryVariantRouting(
   variant: "view" | "served" | "storage" | "rollup",
   outcome: "used" | "skipped",
   reason:
      | "none"
      | "row_level_gate"
      | "not_applicable"
      | "does_not_answer"
      | "run_failed",
): void {
   queryVariantRoutingCounter().add(1, { variant, outcome, reason });
}

/**
 * Record how a chained `storage=` source was built (see
 * {@link ChainedStorageBuildOutcome}). Only fired for a source that actually
 * reads a storage-materialized upstream — a single-source build emits nothing
 * here. The parent_reuse : inline_fallback ratio shows how often stacking on
 * the parent works.
 */
export function recordChainedStorageBuild(
   outcome: ChainedStorageBuildOutcome,
): void {
   chainedStorageBuildCounter().add(1, { outcome });
}

/**
 * Record a build that recomputed an upstream from raw because the upstream's
 * manifest entry is at a different tier than it is declared on now (see
 * {@link staleUpstreamEntryCounter}). Once per run per upstream.
 */
export function recordStaleUpstreamEntry(build: "colocated" | "storage"): void {
   staleUpstreamEntryCounter().add(1, { build });
}

/**
 * Record one colocated serve-manifest entry dropped by
 * `Package.bindColocatedServeManifest`. See {@link colocatedBindDroppedCounter}
 * for why `build_plan_unavailable` is the label worth alerting on.
 */
export function recordColocatedBindDropped(
   reason: "build_plan_unavailable" | "refused",
): void {
   colocatedBindDroppedCounter().add(1, { reason });
}

/** Visible for tests. Drops cached instruments so a fresh MeterProvider can capture emissions. */
export function resetMaterializationTelemetryForTesting(): void {
   resetInstrumentsForTesting();
}
