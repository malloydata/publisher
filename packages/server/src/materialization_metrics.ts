// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Centralized telemetry for materialization builds.
 *
 * Operators need to answer "are builds completing, how long do they take, and
 * where are they failing?" without grepping logs. The run counter and the
 * run-duration histogram both carry `mode` (`auto`|`orchestrated`) and
 * `outcome` (`success`|`partial`|`failed`|`cancelled`), so a dashboard can
 * render auto-run vs orchestrated latency side by side, and a failed run's
 * time apart from a successful one's.
 *
 * Instruments are created lazily for the same reason as {@link ./query_cap_metrics}:
 * one created before `setGlobalMeterProvider` binds to a NoOp meter
 * (https://github.com/open-telemetry/opentelemetry-js/issues/3505).
 */

import {
   type TableKind,
   TIERS,
} from "./service/materialization/domain/vocabulary";
import {
   lazyCounter,
   lazyHistogram,
   resetInstrumentsForTesting,
} from "./telemetry";
import type { components } from "./api";
import type {
   BindDeclineReason,
   ServedCopyProblemReason,
} from "./service/materialization/domain/source_decline";

/**
 * Every label below is a closed union whose values are listed in a const array
 * (the telemetry specification in docs/materialization-v2-plan.md), so a spec can
 * enumerate each one and adding a value is a reviewed change. No instrument
 * here carries a package, environment, source, model, table, destination,
 * connection or query label, and none is filled from a runtime string.
 */
export const MATERIALIZATION_MODES = ["auto", "orchestrated"] as const;
export type MaterializationMode = (typeof MATERIALIZATION_MODES)[number];

/**
 * What a build makes: `materialize` a `#(materialize)` table, `persist` a
 * `#@ persist` table, `rollup` a `#(rollup)` or `#@ preaggregate`
 * rollup, `view` a persisted view, `pair` a pair table (an inlined join_many).
 */
export const BUILD_KINDS = [
   "materialize",
   "persist",
   "rollup",
   "view",
   "query",
   "pair",
] as const satisfies readonly TableKind[];
export type BuildKind = (typeof BUILD_KINDS)[number];

/** Where a table is built: the source's own warehouse, or a `storage=` destination. */
export const BUILD_TIERS = TIERS;
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
   "ledger_entry_malformed",
   // A recorded boundary on a table the run's pre-run read did not hold to
   // its contract (a seed whose run never committed, a destination left).
   "ledger_unheld",
   "plan_error",
   "chained_storage",
   "pair_parent_row",
   "merge_key_not_unique",
   // An incremental extension that could not prove it last read its
   // incremental parent's immediately prior table, so it rebuilds.
   "upstream_generation_skipped",
   // An incremental extension on another watermark than its incremental
   // parent's, rebuilt whenever the parent's table changed.
   "upstream_watermark_differs",
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
 * `publisher_query_route_total{leg}`: the legs, and `live`. `materialized` is
 * the served copy's leg, named as the path report names what it answers
 * (`kind: materialized`).
 */
export const ROUTE_LEGS = [
   "view",
   "query",
   "rollup",
   "materialized",
   "live",
] as const;
type RouteLeg = (typeof ROUTE_LEGS)[number];

/** The route metric's `leg` for a router variant kind (the served copy's is `materialized`). */
export function routeLegOf(
   kind: "view" | "query" | "served" | "rollup" | "live",
): RouteLeg {
   return kind === "served" ? "materialized" : kind;
}
/** `publisher_query_route_total{outcome}`. */
export const ROUTE_OUTCOMES = ["used", "skipped", "fallback"] as const;
type RouteOutcome = (typeof ROUTE_OUTCOMES)[number];
type SkipReason = components["schemas"]["SkipReason"];
/**
 * `publisher_query_route_total{reason}`: `NONE` when used, else the public
 * `SkipReason` the path report shows (one vocabulary for both).
 */
export const ROUTE_REASONS = [
   "NONE",
   "NOT_BUILT",
   "NOT_SERVED",
   "OPTED_OUT",
   "MISSING_FIELD",
   "SHAPE_MISMATCH",
   "ROW_LEVEL_GATE",
   "GRAIN_TOO_COARSE",
   "TIME_ZONE",
   "CROSS_CONNECTION",
   "COUNT_SEMANTICS",
   "STALE",
   "READ_FAILED",
   // Retained SkipReasons, no longer produced: listed so the enum stays covered.
   "WARNED",
   "CONTRACT",
   "RUN_FAILED",
] as const satisfies readonly ("NONE" | SkipReason)[];
export type RouteReason = (typeof ROUTE_REASONS)[number];
// Every public SkipReason is a route reason (the array above lists no other).
const ROUTE_REASONS_COVER_SKIP_REASONS: SkipReason extends RouteReason
   ? true
   : never = true;
void ROUTE_REASONS_COVER_SKIP_REASONS;

/** `publisher_served_copy_bind_total{scope}`. */
export const BIND_SCOPES = ["manifest", "source"] as const;
/** `publisher_served_copy_bind_total{outcome}`, by scope. */
export const BIND_OUTCOMES = [
   "success",
   "failure",
   "timeout",
   "bound",
   "partial",
   "declined",
] as const;
/**
 * Why a table was dropped (`publisher_materialization_drop_tables_total`):
 * `delete`, a caller deleted a storage destination's table; `reclaim`,
 * Publisher dropped a table on its own (a failed run's tables, a table a
 * readable name replaced, an archived or deleted version's tables);
 * `caller`, a caller dropped a package's materialized database table
 * (`drop-materialized-table`), after Publisher's checks passed.
 */
export const DROP_OCCASIONS = ["delete", "reclaim", "caller"] as const;
export type DropOccasion = (typeof DROP_OCCASIONS)[number];

/**
 * What a key check reads (`publisher_materialization_key_check_total{kind}`):
 * `inline_join`, an inlined join_one's key, which fails the build when it
 * repeats; `rollup`, a rollup's join_one key over its base's rows; `parent`, a
 * query source's primary key beside a rollup; `pair_leaf`, a pair table's
 * leaf key; `stored`, a stored table's primary key in its destination.
 */
export const KEY_CHECK_KINDS = [
   "inline_join",
   "rollup",
   "parent",
   "pair_leaf",
   "stored",
] as const;
export type KeyCheckKind = (typeof KEY_CHECK_KINDS)[number];

/**
 * What a key check found: `unique`; `not_unique` (a value repeats, or is NULL
 * where the check forbids it); `not_run` (it threw or could not run, so the
 * key reads as unknown); `reused` (an inline join check skipped because a
 * pass the run holds still describes the joined table).
 */
export const KEY_CHECK_OUTCOMES = [
   "unique",
   "not_unique",
   "not_run",
   "reused",
] as const;
type KeyCheckOutcome = (typeof KEY_CHECK_OUTCOMES)[number];

/**
 * Which tier a table drop ran against (`publisher_materialization_drop_tables_total{tier}`):
 * the source's own database (`database`) or a DuckDB/DuckLake `storage=`
 * destination (`storage`), as {@link BuildTier} names a build's.
 */
type DropTier = BuildTier;
/**
 * Why a valid declaration is not built: its build plan refusal's reason
 * (`RefusedSource.reason`).
 */
export type EligibilityRefusalReason =
   components["schemas"]["RefusedSource"]["reason"];
/**
 * How a chained `storage=` source (one that reads a storage-materialized
 * upstream) was built: `parent_reuse` = built by reading the upstream's stored
 * lake table ("stack on the parent" — reuses the parent's work and is
 * consistent-by-construction); `strict_refused` = no build over its stored
 * upstreams exists (an upstream neither built in the run nor referenced, a
 * path to the warehouse, a shape the build could not carry, a string the
 * destination compares otherwise), so the build failed rather than
 * recomputing its upstreams from raw.
 *
 * Retained label values, no longer produced (no build reads its upstreams'
 * definitions in place of their tables): `inline_fallback` and
 * `strict_shape_fallback`.
 */
type ChainedStorageBuildOutcome =
   | "parent_reuse"
   | "strict_refused"
   // The parent-reuse attempt failed on infrastructure (attach, CTAS, the
   // destination being unreachable) rather than on shape: an outage, not a
   // modelling limit, and the build fails.
   | "infra_failure";

const runCounter = lazyCounter(
   "publisher_materialization_runs_total",
   "Materialization builds completed. Labels: mode ('auto'|'orchestrated'), outcome ('success'|'partial'|'failed'|'cancelled').",
);
const runDuration = lazyHistogram(
   "publisher_materialization_run_duration_ms",
   "Wall-clock duration of a materialization build. Labels: mode ('auto'|'orchestrated'), outcome ('success'|'partial'|'failed'|'cancelled').",
);
const buildCounter = lazyCounter(
   "publisher_materialization_build_total",
   "Source builds, one per source per run at its finish or failure. Labels: " +
      "kind ('materialize'|'persist'|'rollup'|'view'|'query'|'pair'), tier " +
      "('database'|'storage'), outcome ('built'|'reused'|'refused'|'failed'|" +
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
   "Coded findings on authored annotations (the #@ route, and the near-miss and malformed-route scan over every route), counted once each time a package is loaded and accepted for serving (boot, add, publish, reload; a refused publish and a gate re-run count nothing). Labels: code (e.g. 'persist-unknown-key', 'persist-unparseable', 'persist-not-built'), outcome ('invalid' for an error-severity finding, which fails the load, 'not_built' for a warning: a valid annotation not built here, and also the warn-level 'annotation-near-miss' and 'persist-incremental-no-merge-key' advisories, which build).",
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
      "served_copy_bind{reason='build_plan_unavailable'}, which fires only " +
      "per declined entry per load -- a package that loads once and is never " +
      "reloaded ticks that counter once and then sits with its database tier " +
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
const servedCopyBindCounter = lazyCounter(
   "publisher_served_copy_bind_total",
   "Serving's binds. Labels: scope ('manifest': one manifest bind, from REST, " +
      "auto-load or a rebind; 'source': one source deciding whether it " +
      "serves), outcome ('success'|'failure'|'timeout' for a manifest; " +
      "'bound'|'partial'|'declined' for a source: 'partial' is a source that " +
      "serves with a join read as written or left out), reason ('none' when " +
      "bound, else why: 'build_plan_unavailable' when the package's build " +
      "plan failed to compute, so every database source reverts to live; " +
      "'refused'; 'missing_table'; or the served copy's reason, as its " +
      "package warning, 'inline_join_kept' and 'cross_connection_join' being " +
      "the partial ones). Units differ by who counts: 'bound' and the bind's " +
      "declines count once per stored table per bind; the served copy counts " +
      "once per source per model per snapshot it compiles, 'declined' beating " +
      "'partial'. So a source two models read is counted by each, and the " +
      "outcomes do not sum to a number of sources. An extension table that " +
      "falls back to its own blocks ('compile_failed', 'reroot_not_compiled') " +
      "may still serve, and is counted 'declined'.",
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
   "Physical tables dropped. Labels: outcome ('success'|'failure'), tier " +
      "('database'|'storage'), occasion ('delete' when a caller deleted a " +
      "storage destination's table, since a run's delete is record-only; " +
      "'reclaim' when Publisher dropped it on its own: " +
      "a failed run's tables, a table a readable name replaced, an archived " +
      "or deleted version's tables; 'caller' when a caller dropped a " +
      "package's materialized database table through " +
      "DELETE .../materializedTables/{tableName}).",
);
const scheduledFireCounter = lazyCounter(
   "publisher_materialization_scheduled_fires_total",
   "Standalone-scheduler attempts to fire a package's materialization.schedule. " +
      "Label: outcome ('fired'|'conflict'|'error').",
);
const schedulerFireSkippedCounter = lazyCounter(
   "publisher_materialization_scheduler_fires_skipped_total",
   "Standalone-scheduler fires skipped because the schedule's previous fire " +
      "was still running when it came due again. A run of skips for one " +
      "schedule means its fire is stuck; other schedules are not affected.",
);
const queryRouteCounter = lazyCounter(
   "publisher_query_route_total",
   "The router's decision on each leg it considered for a query, counted once " +
      "the query is answered. Labels: leg ('view'|'query'|'rollup'|'materialized'|'live'; " +
      "'query' is a persisted named query), " +
      "outcome ('used'|'skipped'|'fallback': a chosen leg whose run failed and " +
      "was answered live), reason ('NONE' when used, else the path report's " +
      "SkipReason; 'READ_FAILED' with 'fallback'). A leg that does not apply to " +
      "the query is not counted; 'live' is counted 'used' when it answered a " +
      "query the router counted a leg for.",
);
const queryRouteDuration = lazyHistogram(
   "publisher_query_route_duration_ms",
   "Wall-clock time the router spent choosing how to answer a query, for a " +
      "query with at least one serving leg that applied: the row-level veto " +
      "and each leg's compile, up to the leg chosen. Not the query's run. " +
      "Label: leg ('view'|'query'|'rollup'|'materialized'|'live'), the one that " +
      "answered.",
);
const keyCheckCounter = lazyCounter(
   "publisher_materialization_key_check_total",
   "Key checks a run made over the rows it read or wrote. Labels: kind " +
      "('inline_join'|'rollup'|'parent'|'pair_leaf'|'stored'), outcome " +
      "('unique'|'not_unique'|'not_run'|'reused'). An inline_join that is " +
      "not_unique fails its build; every other not_unique or not_run leaves " +
      "the key unknown or repeated, so queries that need it unique are " +
      "answered live.",
);
const contractCaptureCounter = lazyCounter(
   "publisher_materialization_contract_capture_total",
   "Column contracts a build captured over the table it wrote. Labels: tier " +
      "('database'|'storage'), outcome ('captured'|'unread'). An unread " +
      "capture records no contract: the table is not served from until it " +
      "is rebuilt (serve verdict reason contract_missing).",
);
const incompatibleDetectedCounter = lazyCounter(
   "publisher_materialization_incompatible_detected_total",
   "Bound tables a bind's catalog read, or a standalone run's read of the " +
      "tables it would carry, found no longer matching the column contract " +
      "their build recorded, so they answer live until rebuilt (a run " +
      "rebuilds them rather than carry them). " +
      "Labels: trigger ('bind'|'run'), code ('column_gone'|'column_retyped'|" +
      "'collation_changed'|'declared_moved'|...). A failed stored read is " +
      "never counted: it leaves the table unread until that read.",
);
/**
 * Where a query walks the IR for a fact a compile or bind records, because
 * the record does not cover it (an inline `extend`, an unrecorded lane or
 * stored set).
 */
export type QueryWalkSite =
   | "served_copy_declines"
   | "scope"
   | "rollup_candidates"
   | "path_chain";
const queryWalkCounter = lazyCounter(
   "publisher_query_walks_total",
   "IR walks a query made for a fact the served copy's compile or the bind " +
      "records, because the record did not cover the query (an inline " +
      "extend, an unrecorded lane or stored set). Labels: site " +
      "('served_copy_declines'|'scope'|'rollup_candidates'|'path_chain').",
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
   "Valid materialization declarations that are not built (`#@ persist`, " +
      "`#(materialize)`, a persisted view or query, a rollup), once per " +
      "declaration per plan derivation, and the refusals only a run finds. " +
      "Label: reason, the build plan refusal's `RefusedSource.reason` " +
      "(api-doc.yaml lists the values and which are retained and no " +
      "longer produced).",
);
const serveShapeTypeFallbackCounter = lazyCounter(
   "publisher_storage_serve_shape_type_fallback_total",
   "Captured DuckDB column types mapped to json in a stored table's declared " +
      "type (type fidelity loss). " +
      "Label: kind ('array'|'unrecognized').",
);
const storedColumnTypeMismatchCounter = lazyCounter(
   "publisher_storage_column_type_mismatch_total",
   "Stored columns whose DuckDB type does not serve as the type Malloy " +
      "declares for them. Labels: stage ('build' = " +
      "found after a storage build; 'serve' = found when a model's bindings " +
      "are set, then cast on read or left out of the served copy so queries " +
      "reading it are answered live), declared (the Malloy type).",
);
const chainedStorageBuildCounter = lazyCounter(
   "publisher_storage_chained_build_total",
   "Chained storage= source builds (a source reading a storage-materialized " +
      "upstream). Label: outcome ('parent_reuse'|'strict_refused'|" +
      "'infra_failure'; 'inline_fallback' and 'strict_shape_fallback' are " +
      "retained but no longer produced: no build recomputes its upstreams " +
      "from raw).",
);
const uncheckedUpstreamReadCounter = lazyCounter(
   "publisher_materialization_upstream_recompute_total",
   "Builds that read a stored upstream's database table unchecked: a host's " +
      "reference carried no nativeSchema (a storage= one, no contract), so the " +
      "table's columns could not be " +
      "checked against its build. No build recomputes an upstream, so this is " +
      "the only reason and outcome produced. Labels: reason " +
      "('reference_types_unrecorded'), build ('chained' for a build over the " +
      "upstream, 'rollup' for a database rollup over its stored base), and " +
      "outcome ('read_unchecked'). The reasons 'moved', 'unreadable', " +
      "'types_unrecorded' and 'persisted_result' and the outcome 'recomputed' " +
      "are retired: such a build is refused.",
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

/** Record a due standalone-scheduler fire skipped while its previous fire ran. */
export function recordSchedulerFireSkipped(): void {
   schedulerFireSkippedCounter().add(1);
}

/** Record the outcome and duration of a materialization build. */
export function recordMaterializationRun(
   mode: MaterializationMode,
   outcome: MaterializationOutcome,
   durationMs: number,
): void {
   runCounter().add(1, { mode, outcome });
   runDuration().record(durationMs, { mode, outcome });
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
 * one increment per finding: `code` is the warning's code, `outcome` its
 * severity in the warning suffixes' words (`error` is `invalid`, `warn`
 * `not_built`). An `invalid` count can only come from a package published
 * before its refusal existed, since publish refuses an invalid finding before
 * the package is served.
 */
export function recordAnnotationFindings(
   warnings: ReadonlyArray<{ code?: string; severity?: string }>,
): void {
   for (const { code, severity } of warnings) {
      if (!code) continue;
      annotationFindingsCounter().add(1, {
         code,
         outcome: severity === "error" ? "invalid" : "not_built",
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
 * Record an auto-run manifest auto-load attempt. A failure does not fail the
 * run, whose tables were built: the run reaches MANIFEST_FILE_READY with
 * `autoLoad: "failed"` on its metadata and its `error` set, and queries are
 * not answered from the new tables until the package's next reload or restart.
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
   servedCopyBindCounter().add(1, {
      scope: "manifest",
      outcome,
      reason: "none",
   });
}

/** Record one source entry a bind admitted to serve. */
export function recordSourceBound(): void {
   servedCopyBindCounter().add(1, {
      scope: "source",
      outcome: "bound",
      reason: "none",
   });
}

/**
 * Record one source entry declined: by the bind (the plan, its refusal, a
 * table built from another definition) or by the served copy's compile (the
 * reason its package warning gives). `build_plan_unavailable` is the one to
 * alert on: database is the default tier and is not gated by
 * PERSIST_STORAGE_MODE, so every database source of the package reverts to
 * live until a load succeeds.
 */
export function recordSourceDeclined(reason: BindDeclineReason): void {
   servedCopyBindCounter().add(1, {
      scope: "source",
      outcome: "declined",
      reason,
   });
}

/**
 * Record one source the served copy's compile did not serve whole, with the
 * outcome `servedCopyOutcomes` resolved for it: `partial` when it still
 * serves with part of its copy read as written or left out, else `declined`.
 */
export function recordServedCopyOutcome(
   outcome: "partial" | "declined",
   reason: ServedCopyProblemReason,
): void {
   servedCopyBindCounter().add(1, { scope: "source", outcome, reason });
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

/** Record a best-effort physical-table drop, and why it was dropped. */
export function recordDropTables(
   outcome: "success" | "failure",
   tier: DropTier,
   occasion: DropOccasion,
): void {
   dropTablesCounter().add(1, { outcome, tier, occasion });
}

/** Record one key check a run made, and what it found. */
export function recordKeyCheck(
   kind: KeyCheckKind,
   outcome: KeyCheckOutcome,
): void {
   keyCheckCounter().add(1, { kind, outcome });
}

/**
 * Record a table a bind's catalog read found incompatible with its contract
 * (package_materialization.ts), by code. A failed stored read is never
 * counted: it leaves the table unread until that read.
 */
export function recordIncompatibleDetected(
   trigger: "bind" | "run",
   code: string,
): void {
   incompatibleDetectedCounter().add(1, { trigger, code });
}

/** Record one query-path walk for a recorded fact. */
export function recordQueryWalk(site: QueryWalkSite): void {
   queryWalkCounter().add(1, { site });
}

/** Record one column contract capture (run/column_contract_capture.ts). */
export function recordContractCapture(
   tier: BuildTier,
   outcome: "captured" | "unread",
): void {
   contractCaptureCounter().add(1, { tier, outcome });
}

/** Record how long the router took to choose the leg that answered a query. */
export function recordQueryRouteDuration(
   durationMs: number,
   leg: RouteLeg,
): void {
   queryRouteDuration().record(durationMs, { leg });
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
 * Record a materialization-eligibility refusal. A build plan's refusals are
 * counted as it is derived and recorded where the plan is adopted (a load, a
 * run's guard), each source in its `refusedSources` once, under the reason
 * there, and never a source it does not refuse (a database source only
 * unservable from storage); a run records the refusals only it can find. The
 * `given` reason is a security refusal (a frozen given-filtered table would
 * leak rows across tenants); worth tracking how often authors attempt it.
 */
export function recordEligibilityRefused(
   reason: EligibilityRefusalReason,
): void {
   eligibilityRefusedCounter().add(1, { reason });
}

/**
 * Record a captured DuckDB column type that could not map onto a Malloy basic
 * type and was carried as json in a stored table's declared type — a
 * type-fidelity loss specific to the storage tier. `array` = a collection/array type; `unrecognized`
 * = an unknown/nested/struct/enum/blob type.
 */
export function recordStoredTypeFallback(kind: "array" | "unrecognized"): void {
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
 * One leg of a routed query: what the router did with it, and why
 * (`publisher_query_route_total`).
 */
export function recordQueryRoute(
   leg: RouteLeg,
   outcome: RouteOutcome,
   reason: RouteReason,
): void {
   queryRouteCounter().add(1, { leg, outcome, reason });
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
 * Record a build that read a host-referenced upstream table unchecked (see
 * {@link uncheckedUpstreamReadCounter}). Once per build per upstream.
 */
export function recordUpstreamReadUnchecked(build: "chained" | "rollup"): void {
   uncheckedUpstreamReadCounter().add(1, {
      reason: "reference_types_unrecorded",
      build,
      outcome: "read_unchecked",
   });
}

/** Visible for tests. Drops cached instruments so a fresh MeterProvider can capture emissions. */
export function resetMaterializationTelemetryForTesting(): void {
   resetInstrumentsForTesting();
}
