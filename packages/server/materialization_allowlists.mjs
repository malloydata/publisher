// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Materialization's layer and documentation checks start from the violations
// the tree had when they were added. Each entry names the step of the plan
// (docs/materialization-v2-plan.md, section 3) whose change removes it. Every
// list only shrinks: src/import_direction.spec.ts and
// src/service/materialization/layer_rules.spec.ts fail on an entry whose
// violation is gone (delete it) and on a violation no entry names (fix it).

/** Import-direction pairs (R5 to R10, H5), "<importer> -> <module>": step. */
export const MATERIALIZATION_IMPORT_ALLOWLIST = {
   // R8: plan/, run/ and serve/ never import one another. run/ enters plan/
   // through run_plan.ts alone.
   "service/materialization/run/run_plan.ts -> service/materialization/plan/addressing":
      "2m",
   "service/materialization/run/run_plan.ts -> service/materialization/plan/build_file_generator":
      "2m",
   "service/materialization/run/run_plan.ts -> service/materialization/plan/build_plan":
      "2m",
   "service/materialization/run/run_plan.ts -> service/materialization/plan/build_plan_compile":
      "2m",
   "service/materialization/run/run_plan.ts -> service/materialization/plan/materialization_eligibility":
      "2m",
   "service/materialization/run/run_plan.ts -> service/materialization/plan/materialize_stored_chain":
      "2m",
   "service/materialization/run/run_plan.ts -> service/materialization/plan/persist_source_fields":
      "2m",
   "service/materialization/serve/served_copy_serve.ts -> service/materialization/plan/persist_dynamic_terms":
      "2m",
   // R10: outside the module, only index.ts and domain/.
   "controller/materialization.controller.ts -> service/materialization/run/build_upstreams":
      "2n",
   "controller/materialization.controller.ts -> service/materialization/run/materialization_service":
      "2n",
   "controller/storage_destination.controller.ts -> service/materialization/run/storage_admin":
      "2n",
   "main_server.ts -> service/materialization/run/materialization_scheduler":
      "2n",
   "main_server.ts -> service/materialization/run/materialization_service":
      "2n",
   "package_load/package_load_worker.ts -> service/materialization/plan/build_plan":
      "2n",
   "service/catalog_admin.ts -> service/materialization/run/materialization_build_session":
      "2n",
   "service/catalog_admin.ts -> service/materialization/run/storage_admin":
      "2n",
   "service/catalog_admin.ts -> service/materialization/run/storage_table_load":
      "2n",
   "service/environment.ts -> service/materialization/run/manifest_loader":
      "2n",
   "service/environment_store.ts -> service/materialization/run/inline_uniqueness":
      "2n",
   "service/model.ts -> service/materialization/plan/build_plan": "2n",
   "service/model.ts -> service/materialization/plan/persisted_view": "2n",
   "service/model.ts -> service/materialization/plan/preaggregation_validation":
      "2n",
   "service/model.ts -> service/materialization/plan/rollup_plan": "2n",
   "service/model.ts -> service/materialization/serve/rollup_variants": "2n",
   "service/model.ts -> service/materialization/serve/serve_verdict": "2n",
   "service/model.ts -> service/materialization/serve/served_copy_serve": "2n",
   "service/model.ts -> service/materialization/serve/storage_bindings": "2n",
   "service/model.ts -> service/materialization/serve/view_variants": "2n",
   "service/model.ts -> service/materialization/text/source_map": "2n",
   "service/model_policy.ts -> service/materialization/serve/query_path": "2n",
   "service/package.ts -> service/materialization/package_materialization":
      "2n",
   "service/package.ts -> service/materialization/plan/incremental_policy":
      "2n",
   "service/package.ts -> service/materialization/plan/materialization_config_validation":
      "2n",
   "service/package.ts -> service/materialization/plan/persist_annotation_validation":
      "2n",
   "service/model_serving.ts -> service/materialization/serve/leg": "2n",
   "service/model_serving.ts -> service/materialization/serve/query_path": "2n",
   "service/model_serving.ts -> service/materialization/serve/served_copy_serve":
      "2n",
   "service/model.ts -> service/materialization/serve/leg": "2n",
   "service/query_serving.ts -> service/materialization/serve/read_admission":
      "2n",
   "service/query_serving.ts -> service/materialization/serve/leg": "2n",
   "service/query_serving.ts -> service/materialization/serve/query_path": "2n",
   "service/query_serving.ts -> service/materialization/serve/rollup_variants":
      "2n",
   "service/query_serving.ts -> service/materialization/serve/served_copy_serve":
      "2n",
   "service/query_serving.ts -> service/materialization/serve/view_variants":
      "2n",
   "service/query_serving.ts -> service/materialization/text/source_map": "2n",
   "service/skills/package_skills.ts -> service/materialization/run/cron_evaluator":
      "2n",
   // H5: logger and metrics only in run/, the facade and the query pipeline.
   "service/materialization/plan/build_plan.ts -> logger": "2o",
   "service/materialization/plan/build_plan.ts -> materialization_metrics":
      "2o",
   "service/materialization/plan/build_plan_compile.ts -> logger": "2o",
   "service/materialization/plan/build_plan_compile.ts -> materialization_metrics":
      "2o",
   "service/materialization/plan/eligibility_collect.ts -> logger": "2o",
   // H6: plan/, serve/, domain/ and text/ read no setting; the shell reads
   // the switches once and passes them down (2q, the domain model's switches).
   "service/materialization/plan/addressing.ts -> config": "2q",
   "service/materialization/plan/build_plan.ts -> config": "2q",
   "service/materialization/plan/materialization_eligibility.ts -> config":
      "2q",
   "service/materialization/plan/rollup_compile.ts -> config": "2q",
   "service/materialization/plan/rollup_plan.ts -> config": "2q",
   "service/materialization/serve/rollup_variants.ts -> config": "2q",
   "service/materialization/plan/materialize_compile.ts -> logger": "2o",
   "service/materialization/plan/persist_source_fields.ts -> logger": "2o",
   "service/materialization/plan/rollup_compile.ts -> logger": "2o",
   "service/materialization/serve/rollup_variants.ts -> logger": "2o",
   "service/materialization/serve/served_copy_serve.ts -> logger": "2o",
   "service/materialization/serve/view_variants.ts -> logger": "2o",
};

/** H1: modules with no header. None is allowed. */
export const MATERIALIZATION_HEADER_ALLOWLIST = {};

/** H2: ticket numbers, phase or step labels and dates in comments. None is allowed. */
export const MATERIALIZATION_HISTORY_ALLOWLIST = {};

/** H4: exports no other file imports, "<file>#<name>": step. */
export const MATERIALIZATION_UNIMPORTED_EXPORTS = {};
/** Annotation payload reads outside plan/declaration.ts, per file. */
export const RATCHET_ANNOTATION_READS = {};
/** Reads of `_modelDef`, `_query`, `_sourceDef` or `_explore` outside governance/ir, per file. */
export const RATCHET_IR_READS = {};
/** Clock reads in serve/ and domain/, per file. */
export const RATCHET_CLOCK_READS = {};
/**
 * Dialect literals, per file: every string literal that is exactly a
 * registered dialect, every bare object key that is one, and a dialect-named
 * value compared to any other string literal. `case` labels are free in
 * run/warehouse_sql.ts only. Each entry is frozen (`table`): a count that
 * grows is reviewed.
 */
export const RATCHET_DIALECT_LITERALS = {
   // The dialect table itself: the DialectName union, its rows' keys,
   // standardsql's display name, and STORAGE_DIALECT.
   "service/materialization/domain/dialect.ts": { count: 20, step: "table" },
   // The SQL half of the dialect table: its comparisons of a narrowed
   // DialectName, CURRENT_SCHEMA_SQL's two keys and type, DuckDB's own
   // literal in duckdbNameMatch, and Snowflake's in its query-tag and cost
   // statements.
   "service/materialization/run/warehouse_sql.ts": { count: 22, step: "table" },
   // READ_ONLY_DIALECTS: a capability kept as a deny-list, so an unknown
   // dialect is not refused.
   "service/materialization/plan/materialization_eligibility.ts": {
      count: 1,
      step: "table",
   },
   // Connection types (FederatedSourceType, the storage destination types),
   // whose `postgres`, `snowflake` and `duckdb` share a dialect's spelling.
   "service/materialization/run/materialization_build_session.ts": {
      count: 4,
      step: "table",
   },
   // FederatedSourceType's `snowflake`, the one engine whose session the
   // build tags, aligns and reads its cost back from.
   "service/materialization/run/build_attribution.ts": {
      count: 3,
      step: "table",
   },
   // PASSTHROUGH_SOURCE_TYPES, connection types a `storage=` build reads,
   // whose `postgres` and `snowflake` share a dialect's spelling.
   "service/materialization/domain/source_connection_type.ts": {
      count: 2,
      step: "table",
   },
   // TABLE_PATH_OPTIONS, the incremental probe's per-dialect table-path
   // decoding (four keys), and probePostgresVersion's own dialect.
   "service/materialization/run/incremental_apply.ts": {
      count: 5,
      step: "table",
   },
   // The package-local DuckDB connection, by its connection name.
   "service/materialization/run/reclaimer.ts": { count: 1, step: "table" },
   // The read-cost record's engine label, not a dialect.
   "service/materialization/run/build_read_cost.ts": {
      count: 2,
      step: "table",
   },
};
/** Functions over 200 lines, "<file>#<function>": step. */
export const RATCHET_LONG_FUNCTIONS = {};
/** Switch reads outside config.ts, the facade and the query pipeline, per file. */
export const RATCHET_SWITCH_READS = {
   "service/environment.ts": { count: 2, step: "2q" },
   "service/materialization/plan/addressing.ts": { count: 3, step: "2q" },
   "service/materialization/plan/build_plan.ts": { count: 3, step: "2q" },
   "service/materialization/plan/materialization_eligibility.ts": {
      count: 4,
      step: "2q",
   },
   "service/materialization/plan/rollup_compile.ts": { count: 1, step: "2q" },
   "service/materialization/plan/rollup_plan.ts": { count: 2, step: "2q" },
   "service/materialization/run/build_upstreams.ts": { count: 2, step: "2q" },
   "service/materialization/run/instructed_build.ts": { count: 1, step: "2q" },
   "service/materialization/run/manifest_committer.ts": {
      count: 1,
      step: "2q",
   },
   "service/materialization/run/materialization_instructed_run.ts": {
      count: 1,
      step: "2q",
   },
   "service/materialization/run/run_instructions.ts": { count: 1, step: "2q" },
   "service/materialization/run/run_telemetry.ts": { count: 1, step: "2q" },
   "service/materialization/run/source_build.ts": { count: 1, step: "2q" },
   "service/materialization/serve/rollup_variants.ts": { count: 2, step: "2q" },
   "service/materialization/serve_verdicts.ts": { count: 3, step: "2q" },
};
/**
 * Tier decisions outside domain/placement.ts, per file: a tier read off a
 * record that carries it should read the record's `tier`.
 */
export const RATCHET_TIER_DECISIONS = {
   "service/materialization/plan/materialization_eligibility.ts": {
      count: 1,
      step: "2q",
   },
   "service/materialization/plan/eligibility_collect.ts": {
      count: 2,
      step: "2q",
   },
   "service/materialization/plan/refusal_key.ts": { count: 1, step: "2q" },
   "service/materialization/plan/rollup_plan.ts": { count: 2, step: "2q" },
   "service/materialization/run/instructed_build.ts": { count: 2, step: "2q" },
   "service/materialization/run/materialization_service.ts": {
      count: 2,
      step: "2q",
   },
   "service/materialization/run/run_instructions.ts": { count: 3, step: "2q" },
   "service/materialization/run/source_build.ts": { count: 1, step: "2q" },
   "service/materialization/serve/leg.ts": { count: 4, step: "2q" },
   "service/materialization/serve/query_path.ts": { count: 3, step: "2q" },
   "service/materialization/serve/serve_verdict.ts": { count: 1, step: "2q" },
   "service/model.ts": { count: 1, step: "2q" },
};
/** Table-name comparisons outside domain/physical_table.ts, per file. */
export const RATCHET_TABLE_IDENTITY = {
   "service/materialization/plan/eligibility_collect.ts": {
      count: 1,
      step: "2q",
   },
   "service/materialization/run/materialization_instructed_run.ts": {
      count: 1,
      step: "2q",
   },
   "service/materialization/run/reclaimer.ts": { count: 1, step: "2q" },
   "service/materialization/run/warehouse_sql.ts": { count: 1, step: "2q" },
   "service/materialization/serve/query_path.ts": { count: 1, step: "2q" },
};
/** Stored-type regexes outside stored_column_types.ts, per file. */
export const RATCHET_STORED_TYPE_PATTERNS = {
   "service/materialization/domain/admission.ts": { count: 1, step: "2r" },
   "service/materialization/domain/gate_policy.ts": { count: 1, step: "2r" },
   "service/materialization/serve/bind.ts": { count: 4, step: "2r" },
   "service/materialization/serve/leg.ts": { count: 1, step: "2r" },
   "service/materialization/serve/rollup_variants.ts": { count: 1, step: "2r" },
   "service/materialization/serve/served_copy_serve.ts": {
      count: 2,
      step: "2r",
   },
};
/**
 * SQL and connection reads in serve/, per file: the collation probe (behind
 * its port, a file move away from the shell) and the served copy's runtime.
 */
export const RATCHET_SERVE_IO = {
   "service/materialization/serve/gate_collation.ts": { count: 9, step: "2r" },
   "service/materialization/serve/served_copy_config.ts": {
      count: 2,
      step: "2r",
   },
};
/** Gate-policy and admitRead imports outside their owner, "<file>#<name>": step. */
export const RATCHET_ADMISSION_IMPORTS = {
   // The plan's build-time gate rule reads the table rule directly.
   "service/materialization/plan/materialization_eligibility.ts#tableKeepsGates":
      "2r",
   "service/materialization/plan/rollup_plan.ts#tableKeepsGates": "2r",
};
/**
 * Unions and `as const` lists of kind, tier or leg words written out outside
 * domain/vocabulary.ts, per file: each derives from the vocabulary instead.
 */
export const RATCHET_VOCABULARY_UNIONS = {
   "materialization_metrics.ts": { count: 2, step: "2q" },
   "service/materialization/domain/gate_policy.ts": { count: 2, step: "2q" },
   "service/materialization/domain/table_plans.ts": { count: 3, step: "2q" },
   "service/materialization/plan/materialization_eligibility.ts": {
      count: 3,
      step: "2q",
   },
   "service/materialization/plan/materialize_plan.ts": { count: 2, step: "2q" },
   "service/materialization/plan/refusal_key.ts": { count: 1, step: "2q" },
   "service/materialization/run/build_upstreams.ts": { count: 2, step: "2q" },
   "service/materialization/run/materialization_instructed_run.ts": {
      count: 1,
      step: "2q",
   },
   "service/materialization/serve/gate_collation.ts": { count: 1, step: "2q" },
   "service/materialization/serve/leg.ts": { count: 1, step: "2q" },
   "service/materialization/serve/query_path.ts": { count: 3, step: "2q" },
   "service/materialization/serve/serve_verdict.ts": { count: 4, step: "2q" },
   "service/materialization/serve/view_variants.ts": { count: 2, step: "2q" },
   "service/materialization/serve_verdicts.ts": { count: 1, step: "2q" },
};
/**
 * Names docs/materialization-v2-plan.md says were deleted outright that the
 * tree still holds, name: the planned step that deletes them. A name kept for
 * good is not a deleted name and is not listed.
 */
export const DELETED_NAMES_PENDING = {};
