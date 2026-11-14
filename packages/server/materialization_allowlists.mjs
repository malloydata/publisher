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
   // R7: plan/ never imports run/, serve/ or service/model.ts.
   "service/materialization/plan/build_plan_compile.ts -> service/model": "2b",
   "service/materialization/plan/incremental_policy.ts -> service/materialization/run/incremental_apply":
      "2l",
   "service/materialization/plan/materialize_compile.ts -> service/materialization/run/inline_uniqueness":
      "2i",
   // R8: plan/, run/ and serve/ never import one another.
   "service/materialization/run/incremental_apply.ts -> service/materialization/plan/incremental_declaration":
      "2g.3",
   "service/materialization/run/incremental_apply.ts -> service/materialization/plan/rolling_window":
      "2g.3",
   "service/materialization/run/incremental_build.ts -> service/materialization/plan/build_plan":
      "2g.3",
   "service/materialization/run/incremental_build.ts -> service/materialization/plan/incremental_declaration":
      "2g.3",
   "service/materialization/run/incremental_storage.ts -> service/materialization/plan/build_plan":
      "2g.3",
   "service/materialization/run/inline_uniqueness.ts -> service/materialization/plan/build_file_generator":
      "2g.3",
   "service/materialization/run/instructed_build.ts -> service/materialization/plan/addressing":
      "2g.3",
   "service/materialization/run/instructed_build.ts -> service/materialization/plan/build_file_generator":
      "2g.3",
   "service/materialization/run/instructed_build.ts -> service/materialization/plan/build_plan":
      "2g.3",
   "service/materialization/run/instructed_build.ts -> service/materialization/plan/materialization_eligibility":
      "2g.3",
   "service/materialization/run/instructed_build.ts -> service/materialization/plan/rollup_plan":
      "2g.3",
   "service/materialization/run/materialization_build_session.ts -> service/materialization/plan/build_plan":
      "2g.3",
   "service/materialization/run/materialization_build_session.ts -> service/materialization/serve/materialization_serve_transform":
      "R",
   "service/materialization/run/materialization_instructed_run.ts -> service/materialization/plan/build_plan":
      "2g.3",
   "service/materialization/run/materialization_run_support.ts -> service/materialization/plan/build_file_generator":
      "2g.3",
   "service/materialization/run/materialization_run_support.ts -> service/materialization/plan/build_plan":
      "2g.3",
   "service/materialization/run/materialization_run_support.ts -> service/materialization/plan/persist_partition":
      "2g.3",
   "service/materialization/run/materialization_run_support.ts -> service/materialization/plan/persist_source_fields":
      "2g.3",
   "service/materialization/run/materialization_run_support.ts -> service/materialization/serve/materialization_serve_transform":
      "R",
   "service/materialization/run/materialization_service.ts -> service/materialization/plan/build_plan":
      "2g.3",
   "service/materialization/run/materialization_service.ts -> service/materialization/plan/build_plan_compile":
      "2g.3",
   "service/materialization/run/name_view.ts -> service/materialization/plan/build_plan":
      "2g.3",
   "service/materialization/run/run_instructions.ts -> service/materialization/plan/build_plan":
      "2g.3",
   "service/materialization/run/run_instructions.ts -> service/materialization/plan/materialization_eligibility":
      "2g.3",
   "service/materialization/run/run_telemetry.ts -> service/materialization/plan/build_plan":
      "2g.3",
   "service/materialization/run/run_telemetry.ts -> service/materialization/plan/persisted_view":
      "2g.3",
   "service/materialization/run/source_build.ts -> service/materialization/plan/build_plan":
      "2g.3",
   "service/materialization/run/storage_build_strategy.ts -> service/materialization/plan/build_file_generator":
      "2g.3",
   "service/materialization/run/storage_build_strategy.ts -> service/materialization/serve/materialization_serve_transform":
      "R",
   "service/materialization/run/table_ownership.ts -> service/materialization/plan/build_plan":
      "2g.3",
   "service/materialization/run/table_ownership.ts -> service/materialization/plan/persist_source_fields":
      "2g.3",
   "service/materialization/serve/author_imports.ts -> service/materialization/plan/persisted_view":
      "2g.2",
   "service/materialization/serve/colocated_bindings.ts -> service/materialization/plan/build_file_generator":
      "2g.2",
   "service/materialization/serve/declaring_file_variant.ts -> service/materialization/plan/persisted_view":
      "2g.2",
   "service/materialization/serve/join_rollup.ts -> service/materialization/plan/rollup_measure_grammar":
      "2g.2",
   "service/materialization/serve/join_rollup.ts -> service/materialization/plan/rollup_plan":
      "2g.2",
   "service/materialization/serve/rollup_variants.ts -> service/materialization/plan/persisted_view":
      "2g.2",
   "service/materialization/serve/rollup_variants.ts -> service/materialization/plan/persisted_view_compile":
      "2g.2",
   "service/materialization/serve/rollup_variants.ts -> service/materialization/plan/rollup_compile":
      "2g.2",
   "service/materialization/serve/rollup_variants.ts -> service/materialization/plan/rollup_measure_grammar":
      "2g.2",
   "service/materialization/serve/rollup_variants.ts -> service/materialization/plan/rollup_plan":
      "2g.2",
   "service/materialization/serve/served_copy.ts -> service/materialization/plan/build_file_generator":
      "2g.2",
   "service/materialization/serve/served_copy_containment.ts -> service/materialization/plan/cross_connection_joins":
      "2g.2",
   "service/materialization/serve/served_copy_serve.ts -> service/materialization/plan/build_file_generator":
      "2g.2",
   "service/materialization/serve/served_copy_serve.ts -> service/materialization/plan/cross_connection_joins":
      "2g.2",
   "service/materialization/serve/served_copy_serve.ts -> service/materialization/plan/persist_dynamic_terms":
      "2g.2",
   "service/materialization/serve/served_copy_serve.ts -> service/materialization/plan/rolling_window":
      "2g.2",
   "service/materialization/serve/view_variants.ts -> service/materialization/plan/persisted_view":
      "2g.2",
   "service/materialization/serve/view_variants.ts -> service/materialization/plan/persisted_view_compile":
      "2g.2",
   // R10: outside the module, only index.ts and domain/.
   "controller/materialization.controller.ts -> service/materialization/run/materialization_run_support":
      "2g.3",
   "controller/materialization.controller.ts -> service/materialization/run/materialization_service":
      "2g.3",
   "controller/storage_destination.controller.ts -> service/materialization/run/materialization_build_session":
      "2g.3",
   "main_server.ts -> service/materialization/run/materialization_scheduler":
      "2g.3",
   "main_server.ts -> service/materialization/run/materialization_service":
      "2g.3",
   "package_load/package_load_worker.ts -> service/materialization/plan/build_plan":
      "2g.3",
   "package_load/package_load_worker.ts -> service/materialization/plan/cross_connection_joins":
      "2g.3",
   "package_load/package_load_worker.ts -> service/materialization/plan/preaggregation_compile":
      "2g.3",
   "package_load/protocol.ts -> service/materialization/plan/build_plan":
      "2g.3",
   "service/catalog_admin.ts -> service/materialization/run/materialization_build_session":
      "2g.3",
   "service/catalog_admin.ts -> service/materialization/run/storage_table_load":
      "2g.3",
   "service/environment.ts -> service/materialization/run/manifest_loader":
      "2g.3",
   "service/environment_store.ts -> service/materialization/run/inline_uniqueness":
      "2g.3",
   "service/model.ts -> service/materialization/plan/build_file_generator":
      "2g.2",
   "service/model.ts -> service/materialization/plan/build_plan": "2g.2",
   "service/model.ts -> service/materialization/plan/cross_connection_joins":
      "2g.2",
   "service/model.ts -> service/materialization/plan/preaggregation_validation":
      "2g.2",
   "service/model.ts -> service/materialization/serve/materialization_serve_transform":
      "2g.2",
   "service/model.ts -> service/materialization/serve/query_path": "2g.2",
   "service/model.ts -> service/materialization/serve/rollup_variants": "2g.2",
   "service/model.ts -> service/materialization/serve/serve_verdict": "2g.2",
   "service/model.ts -> service/materialization/serve/served_copy_serve":
      "2g.2",
   "service/model.ts -> service/materialization/serve/storage_bindings": "2g.2",
   "service/model.ts -> service/materialization/serve/view_variants": "2g.2",
   "service/model.ts -> service/materialization/text/source_map": "2g.2",
   "service/model_policy.ts -> service/materialization/serve/query_path":
      "2g.2",
   "service/package.ts -> service/materialization/package_materialization":
      "2g.3",
   "service/package.ts -> service/materialization/plan/build_plan": "2g.3",
   "service/package.ts -> service/materialization/plan/incremental_policy":
      "2g.3",
   "service/package.ts -> service/materialization/plan/materialization_config_validation":
      "2g.3",
   "service/package.ts -> service/materialization/plan/persist_annotation_validation":
      "2g.3",
   "service/query_serving.ts -> service/materialization/serve/materialization_serve_transform":
      "2g.2",
   "service/query_serving.ts -> service/materialization/serve/query_path":
      "2g.2",
   "service/query_serving.ts -> service/materialization/serve/rollup_variants":
      "2g.2",
   "service/query_serving.ts -> service/materialization/serve/served_copy_serve":
      "2g.2",
   "service/query_serving.ts -> service/materialization/serve/storage_bindings":
      "2g.2",
   "service/query_serving.ts -> service/materialization/serve/view_variants":
      "2g.2",
   "service/query_serving.ts -> service/materialization/text/source_map":
      "2g.2",
   "service/skills/package_skills.ts -> service/materialization/run/cron_evaluator":
      "2g.3",
   // H5: logger and metrics only in run/, the facade and the query pipeline.
   "service/materialization/plan/build_plan.ts -> logger": "2g.3",
   "service/materialization/plan/build_plan.ts -> materialization_metrics":
      "2g.3",
   "service/materialization/plan/build_plan_compile.ts -> logger": "2g.3",
   "service/materialization/plan/build_plan_compile.ts -> materialization_metrics":
      "2g.3",
   "service/materialization/plan/eligibility_collect.ts -> logger": "2g.3",
   "service/materialization/plan/materialization_eligibility.ts -> materialization_metrics":
      "2g.3",
   "service/materialization/plan/materialize_compile.ts -> logger": "2g.3",
   "service/materialization/plan/persist_source_fields.ts -> logger": "2g.3",
   "service/materialization/plan/persisted_view_compile.ts -> logger": "2g.3",
   "service/materialization/plan/rollup_compile.ts -> logger": "2g.3",
   "service/materialization/serve/materialization_serve_transform.ts -> logger":
      "R",
   "service/materialization/serve/materialization_serve_transform.ts -> materialization_metrics":
      "R",
   "service/materialization/serve/rollup_variants.ts -> logger": "2g.2",
   "service/materialization/serve/served_copy_serve.ts -> logger": "2g.2",
   "service/materialization/serve/storage_bindings.ts -> logger": "2g.2",
   "service/materialization/serve/storage_bindings.ts -> materialization_metrics":
      "2g.2",
   "service/materialization/serve/view_variants.ts -> logger": "2g.2",
};

/** H1: modules with no header yet, by file under src/: step. */
export const MATERIALIZATION_HEADER_ALLOWLIST = {
   "service/materialization/package_materialization.ts": "5",
   "service/materialization/plan/build_plan.ts": "5",
   "service/materialization/plan/incremental_declaration.ts": "5",
   "service/materialization/plan/incremental_policy.ts": "5",
   "service/materialization/plan/materialization_eligibility.ts": "5",
   "service/materialization/plan/persist_annotation_validation.ts": "5",
   "service/materialization/plan/persist_dynamic_terms.ts": "5",
   "service/materialization/plan/persist_partition.ts": "5",
   "service/materialization/run/build_failure_class.ts": "5",
   "service/materialization/run/build_query_tag.ts": "5",
   "service/materialization/run/build_read_cost.ts": "5",
   "service/materialization/run/build_strategy.ts": "5",
   "service/materialization/run/colocated_build_strategy.ts": "5",
   "service/materialization/run/error_code.ts": "5",
   "service/materialization/run/incremental_apply.ts": "5",
   "service/materialization/run/incremental_build.ts": "5",
   "service/materialization/run/incremental_delta_strategy.ts": "5",
   "service/materialization/run/incremental_storage.ts": "5",
   "service/materialization/run/instructed_build.ts": "5",
   "service/materialization/run/manifest_committer.ts": "5",
   "service/materialization/run/manifest_loader.ts": "5",
   "service/materialization/run/materialization_build_session.ts": "5",
   "service/materialization/run/materialization_instructed_run.ts": "5",
   "service/materialization/run/materialization_run_support.ts": "5",
   "service/materialization/run/materialization_scheduler.ts": "5",
   "service/materialization/run/materialization_service.ts": "5",
   "service/materialization/run/name_view.ts": "5",
   "service/materialization/run/name_view_sql.ts": "5",
   "service/materialization/run/reclaimer.ts": "5",
   "service/materialization/run/run_instructions.ts": "5",
   "service/materialization/run/run_lifecycle.ts": "5",
   "service/materialization/run/run_telemetry.ts": "5",
   "service/materialization/run/same_catalog_build.ts": "5",
   "service/materialization/run/source_build.ts": "5",
   "service/materialization/run/storage_build_strategy.ts": "5",
   "service/materialization/run/storage_table_load.ts": "5",
   "service/materialization/run/table_claims.ts": "5",
   "service/materialization/run/table_ownership.ts": "5",
   "service/materialization/serve/materialization_serve_transform.ts": "5",
};

/** H2: ticket numbers, phase or step labels and dates in comments, per file: the count allowed, and the step that removes them. */
export const MATERIALIZATION_HISTORY_ALLOWLIST = {
   "service/materialization/package_materialization.ts": {
      count: 8,
      step: "5",
   },
   "service/materialization/plan/addressing.ts": { count: 1, step: "5" },
   "service/materialization/plan/build_file_generator.ts": {
      count: 8,
      step: "5",
   },
   "service/materialization/plan/build_plan.ts": { count: 8, step: "5" },
   "service/materialization/plan/build_plan_compile.ts": {
      count: 2,
      step: "5",
   },
   "service/materialization/plan/cross_connection_joins.ts": {
      count: 5,
      step: "5",
   },
   "service/materialization/plan/eligibility_collect.ts": {
      count: 4,
      step: "5",
   },
   "service/materialization/plan/materialization_eligibility.ts": {
      count: 1,
      step: "5",
   },
   "service/materialization/plan/persisted_view.ts": { count: 2, step: "5" },
   "service/materialization/plan/preaggregation_synthesis.ts": {
      count: 1,
      step: "5",
   },
   "service/materialization/plan/rolling_window.ts": { count: 2, step: "5" },
   "service/materialization/plan/rollup_compile.ts": { count: 2, step: "5" },
   "service/materialization/plan/rollup_measure_grammar.ts": {
      count: 1,
      step: "5",
   },
   "service/materialization/plan/rollup_plan.ts": { count: 5, step: "5" },
   "service/materialization/plan/v1_build_companion.ts": {
      count: 3,
      step: "5",
   },
   "service/materialization/run/incremental_build.ts": { count: 1, step: "5" },
   "service/materialization/run/instructed_build.ts": { count: 6, step: "5" },
   "service/materialization/run/materialization_build_session.ts": {
      count: 2,
      step: "5",
   },
   "service/materialization/run/materialization_instructed_run.ts": {
      count: 1,
      step: "5",
   },
   "service/materialization/run/materialization_run_support.ts": {
      count: 7,
      step: "5",
   },
   "service/materialization/run/materialization_service.ts": {
      count: 3,
      step: "5",
   },
   "service/materialization/run/name_view.ts": { count: 1, step: "5" },
   "service/materialization/run/name_view_sql.ts": { count: 1, step: "5" },
   "service/materialization/run/run_instructions.ts": { count: 1, step: "5" },
   "service/materialization/run/source_build.ts": { count: 4, step: "5" },
   "service/materialization/run/storage_build_strategy.ts": {
      count: 7,
      step: "5",
   },
   "service/materialization/serve/author_imports.ts": { count: 1, step: "5" },
   "service/materialization/serve/colocated_bindings.ts": {
      count: 1,
      step: "5",
   },
   "service/materialization/serve/join_rollup.ts": { count: 6, step: "5" },
   "service/materialization/serve/materialization_serve_transform.ts": {
      count: 2,
      step: "5",
   },
   "service/materialization/serve/rollup_variants.ts": { count: 3, step: "5" },
   "service/materialization/serve/serve_verdict.ts": { count: 8, step: "5" },
   "service/materialization/serve/served_copy.ts": { count: 3, step: "5" },
   "service/materialization/serve/served_copy_serve.ts": {
      count: 7,
      step: "5",
   },
   "service/materialization/serve/storage_bindings.ts": { count: 1, step: "5" },
   "service/materialization/serve/view_variants.ts": { count: 1, step: "5" },
   "service/materialization/stored_column_types.ts": { count: 2, step: "5" },
};
/** H4: exports no other file imports, "<file>#<name>": step. */
export const MATERIALIZATION_UNIMPORTED_EXPORTS = {
   "service/materialization/domain/omitted_sources.ts#OmittedFile": "2b",
   "service/materialization/domain/omitted_sources.ts#OmittedSource": "2b",
   "service/materialization/plan/build_plan.ts#PersistSourceGateOutcome":
      "2g.3",
   "service/materialization/plan/build_plan.ts#collidingRollups": "2g.3",
   "service/materialization/plan/build_plan.ts#sourceBuildOf": "2g.3",
   "service/materialization/plan/eligibility_collect.ts#sharedTables": "2g.3",
   "service/materialization/plan/incremental_declaration.ts#IncrementalStrategy":
      "2g.3",
   "service/materialization/plan/persisted_view_compile.ts#GeneratedPersistedViews":
      "2g.3",
   "service/materialization/plan/persisted_view_compile.ts#persistedViewRefusalReason":
      "2g.3",
   "service/materialization/plan/preaggregation_synthesis.ts#compareRollupBreadth":
      "2g.3",
   "service/materialization/plan/preaggregation_synthesis.ts#persistNamespace":
      "2g.3",
   "service/materialization/plan/rollup_compile.ts#GeneratedRollups": "2g.3",
   "service/materialization/plan/rollup_compile.ts#rollupCompanionURL": "2g.3",
   "service/materialization/plan/v1_build_companion.ts#PERSISTENCE_FLAG_LINE":
      "2g.3",
   "service/materialization/plan/v1_build_companion.ts#declaresV1Persist":
      "2g.3",
   "service/materialization/run/incremental_apply.ts#IncrementalStepReasonCode":
      "2g.4",
   "service/materialization/run/incremental_apply.ts#pairDeltaStatements":
      "2g.4",
   "service/materialization/run/incremental_build.ts#planSourceRefresh": "2g.4",
   "service/materialization/run/table_ownership.ts#OWNED_TABLE_MARK": "2g.4",
   "service/materialization/serve/declaring_file_variant.ts#gateLines": "2g.2",
   "service/materialization/serve/rollup_variants.ts#checkCoverage": "2g.2",
   "service/materialization/stored_column_types.ts#faithfulCastTarget": "2j",
};
/** Annotation payload reads outside plan/declaration.ts, per file. */
export const RATCHET_ANNOTATION_READS = {
   "service/materialization/plan/build_file_generator.ts": {
      count: 3,
      step: "2f",
   },
   "service/materialization/plan/build_plan.ts": { count: 3, step: "2f" },
   "service/materialization/plan/incremental_declaration.ts": {
      count: 1,
      step: "2f",
   },
   "service/materialization/plan/persist_dynamic_terms.ts": {
      count: 1,
      step: "2f",
   },
   "service/materialization/plan/persist_source_fields.ts": {
      count: 1,
      step: "2f",
   },
   "service/materialization/plan/persisted_view.ts": { count: 3, step: "2f" },
   "service/materialization/plan/plan_layers.ts": { count: 2, step: "2f" },
   "service/materialization/plan/preaggregation_annotation.ts": {
      count: 1,
      step: "2f",
   },
   "service/materialization/plan/preaggregation_synthesis.ts": {
      count: 1,
      step: "2f",
   },
   "service/materialization/plan/rollup_plan.ts": { count: 1, step: "2f" },
   "service/materialization/serve/rollup_variants.ts": { count: 1, step: "2f" },
};
/** Reads of `_modelDef`, `_query`, `_sourceDef` or `_explore` outside governance/ir, per file. */
export const RATCHET_IR_READS = {
   "service/materialization/plan/build_plan.ts": { count: 10, step: "2c" },
   "service/materialization/plan/build_plan_compile.ts": {
      count: 1,
      step: "2c",
   },
   "service/materialization/plan/incremental_declaration.ts": {
      count: 1,
      step: "2c",
   },
   "service/materialization/plan/materialization_eligibility.ts": {
      count: 4,
      step: "2c",
   },
   "service/materialization/plan/materialize_compile.ts": {
      count: 2,
      step: "2c",
   },
   "service/materialization/plan/persist_dynamic_terms.ts": {
      count: 3,
      step: "2c",
   },
   "service/materialization/plan/persist_partition.ts": {
      count: 1,
      step: "2c",
   },
   "service/materialization/plan/persist_source_fields.ts": {
      count: 1,
      step: "2c",
   },
   "service/materialization/run/materialization_build_session.ts": {
      count: 1,
      step: "2c",
   },
   "service/materialization/run/source_build.ts": { count: 1, step: "2c" },
   "service/materialization/run/storage_build_strategy.ts": {
      count: 2,
      step: "2c",
   },
   "service/materialization/serve/declaring_file_variant.ts": {
      count: 2,
      step: "2c",
   },
   "service/materialization/serve/served_copy_serve.ts": {
      count: 1,
      step: "2c",
   },
   "service/materialization/stored_column_types.ts": { count: 2, step: "2c" },
};
/** Clock reads in serve/ and domain/, per file. */
export const RATCHET_CLOCK_READS = {
   "service/materialization/serve/rollup_variants.ts": {
      count: 1,
      step: "2g.1",
   },
   "service/materialization/serve/served_copy_serve.ts": {
      count: 2,
      step: "2g.1",
   },
   "service/materialization/serve/view_variants.ts": { count: 1, step: "2g.1" },
};
/** A dialect compared to a string literal outside the dialect table, per file. */
export const RATCHET_DIALECT_LITERALS = {
   "service/materialization/plan/materialize_compile.ts": {
      count: 1,
      step: "2l",
   },
   "service/materialization/run/colocated_build_strategy.ts": {
      count: 2,
      step: "2l",
   },
   "service/materialization/run/colocated_native_types.ts": {
      count: 3,
      step: "2l",
   },
   "service/materialization/run/incremental_apply.ts": {
      count: 12,
      step: "2l",
   },
   "service/materialization/run/incremental_build.ts": { count: 2, step: "2l" },
   "service/materialization/run/name_view.ts": { count: 1, step: "2l" },
   "service/materialization/run/name_view_sql.ts": { count: 8, step: "2l" },
   "service/materialization/run/table_ownership.ts": { count: 4, step: "2l" },
   "service/materialization/serve/colocated_bindings.ts": {
      count: 1,
      step: "2l",
   },
   "service/materialization/stored_column_types.ts": { count: 4, step: "2l" },
};
/** Functions over 200 lines, "<file>#<function>": step. */
export const RATCHET_LONG_FUNCTIONS = {
   "service/materialization/plan/build_file_generator.ts#planModelMaterialize":
      "2g.3",
   "service/materialization/plan/build_plan.ts#collectModelBuildPlan": "2g.3",
   "service/materialization/plan/build_plan.ts#deriveBuildPlan": "2g.3",
   "service/materialization/plan/materialize_compile.ts#tryCompileMaterializeCompanion":
      "2g.3",
   "service/materialization/plan/materialize_compile.ts#withBuildCopies":
      "2g.3",
   "service/materialization/plan/preaggregation_validation.ts#validateSourcePreaggregation":
      "2g.3",
   "service/materialization/plan/rollup_plan.ts#planSourceRollups": "2g.3",
   "service/materialization/run/instructed_build.ts#InstructedBuild.buildInstructedSource":
      "2g.4",
   "service/materialization/run/instructed_build.ts#InstructedBuild.run":
      "2g.4",
   "service/materialization/run/materialization_build_session.ts#buildSourceIntoStorage":
      "2g.4",
   "service/materialization/run/materialization_service.ts#MaterializationService.buildRun":
      "2g.4",
   "service/materialization/run/materialization_service.ts#MaterializationService.createMaterialization":
      "2g.4",
   "service/materialization/run/reclaimer.ts#Reclaimer.dropManifestEntries":
      "2g.4",
   "service/materialization/run/source_build.ts#SourceBuilder.build": "2g.4",
   "service/materialization/run/storage_build_strategy.ts#StorageBuildStrategy.build":
      "2g.4",
   "service/materialization/serve/query_path.ts#buildQueryPath": "2g.2",
   "service/materialization/serve/rollup_variants.ts#RollupVariantServe.attemptJoin":
      "2g.2",
   "service/materialization/serve/serve_verdict.ts#verdictFor": "2g.2",
   "service/materialization/serve/served_copy_serve.ts#ServedCopyServe.answer":
      "2g.2",
   "service/materialization/serve/served_copy_serve.ts#ServedCopyServe.compile":
      "2g.2",
   "service/materialization/serve/served_copy_serve.ts#ServedCopyServe.rerootFor":
      "2g.2",
   "service/materialization/serve/served_copy_serve.ts#pairRerootOf": "2g.2",
   "service/model.ts#Model.runNotebookCell": "2g.2",
   "service/model.ts#Model.runQueryResults": "2g.2",
};
