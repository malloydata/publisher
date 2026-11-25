// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A drive-less `file:///…` URL in a spec or test helper (the lint rule
// `publisher/no-driveless-file-url`, eslint.config.mjs), at the start of a
// string or inside one, as in a source ID (`m@file:///pkg/m.malloy`). On
// Windows such a URL names no path: `fileURLToPath` throws and a cache keyed
// by path misses, so a spec that turns one into a path fails there alone.
// Build the URL with testFileURL, and the source ID with testSourceID
// (src/test_helpers/test_file_url.ts), instead.
//
// DRIVELESS_FILE_URL_BASELINE is the files that spelled one when the rule was
// added, repository-relative. What is left is opaque keys never read as a
// path (an InMemoryURLReader's map, a source ID or location compared as a
// string), so it is harmless today, but the list only shrinks:
// packages/server/src/driveless_file_urls.spec.ts fails on an entry whose
// file no longer spells one (delete it) and on a file that spells one and is
// not listed (use the helper; do not add it).

/**
 * Whether a string's text, or one piece of a template literal's text, spells
 * a drive-less file URL anywhere: `file:///` followed by anything but a drive
 * letter (`file:///C:/…`). A `file:///` that ends the text counts only when a
 * substitution follows it (`file:///${name}`, `m@file:///${file}`), since
 * alone it is a prefix to compare with, not a location.
 */
export function isDrivelessFileURL(text, substituted) {
   for (
      let at = text.indexOf("file:///");
      at >= 0;
      at = text.indexOf("file:///", at + 1)
   ) {
      const rest = text.slice(at + "file:///".length);
      if (rest === "" ? substituted : !/^[A-Za-z]:/.test(rest)) return true;
   }
   return false;
}

/** Repository-relative files allowed to keep their drive-less file URLs. */
export const DRIVELESS_FILE_URL_BASELINE = [
   "packages/sdk/src/components/ReportBuilder/scannerShapes.spec.ts",
   "packages/server/src/build_plan_omitted_sources_contract.spec.ts",
   "packages/server/src/controller/package.controller.spec.ts",
   "packages/server/src/mcp/tools/compile_tool.spec.ts",
   "packages/server/src/package_load/job_files.spec.ts",
   "packages/server/src/package_relative_text.spec.ts",
   "packages/server/src/service/annotation_near_miss.spec.ts",
   "packages/server/src/service/annotation_ownership.spec.ts",
   "packages/server/src/service/annotation_reader.spec.ts",
   "packages/server/src/service/annotations.spec.ts",
   "packages/server/src/service/authorize_grammar.spec.ts",
   "packages/server/src/service/build_targets_given_binding.spec.ts",
   "packages/server/src/service/caller_joins.spec.ts",
   "packages/server/src/service/compile_document.spec.ts",
   "packages/server/src/service/connection_config.spec.ts",
   "packages/server/src/service/drill_probe.spec.ts",
   "packages/server/src/service/entities/field_helpers.spec.ts",
   "packages/server/src/service/entities/model_text.spec.ts",
   "packages/server/src/service/entities/source_info_serializer.spec.ts",
   "packages/server/src/service/environment_write_model_file.spec.ts",
   "packages/server/src/service/exports_probe.spec.ts",
   "packages/server/src/service/filter_binding_guard.spec.ts",
   "packages/server/src/service/gate_registry_walk.spec.ts",
   "packages/server/src/service/gate_walk_differential.spec.ts",
   "packages/server/src/service/given_required.spec.ts",
   "packages/server/src/service/governance/ir/declared_notes.spec.ts",
   "packages/server/src/service/governance/ir/source_graph.spec.ts",
   "packages/server/src/service/governance/ir/source_reads.spec.ts",
   "packages/server/src/service/malloy_annotation_invariants.spec.ts",
   "packages/server/src/service/malloy_text.spec.ts",
   "packages/server/src/service/managed_catalog_scope_postgres.spec.ts",
   "packages/server/src/service/materialization/admission_parity.spec.ts",
   "packages/server/src/service/materialization/database_table_unread.spec.ts",
   "packages/server/src/service/materialization/dialect_capabilities_characterization.spec.ts",
   "packages/server/src/service/materialization/domain/column_contract.spec.ts",
   "packages/server/src/service/materialization/domain/same_catalog_build.spec.ts",
   "packages/server/src/service/materialization/domain/upstream_schemas.spec.ts",
   "packages/server/src/service/materialization/package_materialization_capability_replan.spec.ts",
   "packages/server/src/service/materialization/package_warning_fields.spec.ts",
   "packages/server/src/service/materialization/plan/addressing_destination.spec.ts",
   "packages/server/src/service/materialization/plan/build_file_generator.spec.ts",
   "packages/server/src/service/materialization/plan/build_plan_compile.spec.ts",
   "packages/server/src/service/materialization/plan/build_plan_gate_classification.spec.ts",
   "packages/server/src/service/materialization/plan/build_plan_omitted_sources.spec.ts",
   "packages/server/src/service/materialization/plan/declaration.spec.ts",
   "packages/server/src/service/materialization/plan/extension_chain_verdict.spec.ts",
   "packages/server/src/service/materialization/plan/freshness_window_message.spec.ts",
   "packages/server/src/service/materialization/plan/load_gate_text.spec.ts",
   "packages/server/src/service/materialization/plan/materialization_eligibility.spec.ts",
   "packages/server/src/service/materialization/plan/materialize_build_copy.spec.ts",
   "packages/server/src/service/materialization/plan/materialize_plan.spec.ts",
   "packages/server/src/service/materialization/plan/materialize_read_time.spec.ts",
   "packages/server/src/service/materialization/plan/materialize_stored_chain.spec.ts",
   "packages/server/src/service/materialization/plan/persisted_view.spec.ts",
   "packages/server/src/service/materialization/plan/preaggregation_validation.spec.ts",
   "packages/server/src/service/materialization/plan/refusal_findings_characterization.spec.ts",
   "packages/server/src/service/materialization/plan/refusal_record.spec.ts",
   "packages/server/src/service/materialization/plan/rolling_window.spec.ts",
   "packages/server/src/service/materialization/plan/rollup_plan.spec.ts",
   "packages/server/src/service/materialization/plan/twin_addresses.spec.ts",
   "packages/server/src/service/materialization/plan/upstream_schema_address.spec.ts",
   "packages/server/src/service/materialization/plan/v1_rollup_plan.spec.ts",
   "packages/server/src/service/materialization/preaggregate_not_built.spec.ts",
   "packages/server/src/service/materialization/run/build_failure_class.spec.ts",
   "packages/server/src/service/materialization/run/build_upstreams.spec.ts",
   "packages/server/src/service/materialization/run/create_refusal_reasons.spec.ts",
   "packages/server/src/service/materialization/run/incremental_recast.spec.ts",
   "packages/server/src/service/materialization/run/incremental_storage_rename.spec.ts",
   "packages/server/src/service/materialization/run/instructed_every_source_accounted.spec.ts",
   "packages/server/src/service/materialization/run/instructed_failure_record.spec.ts",
   "packages/server/src/service/materialization/run/instructed_refused_sibling.spec.ts",
   "packages/server/src/service/materialization/run/key_check_null_rule.spec.ts",
   "packages/server/src/service/materialization/run/materialization_service_lifecycle.spec.ts",
   "packages/server/src/service/materialization/run/run_telemetry.spec.ts",
   "packages/server/src/service/materialization/run/self_table_naming.spec.ts",
   "packages/server/src/service/materialization/run/storage_extension_build.spec.ts",
   "packages/server/src/service/materialization/run/table_ownership.spec.ts",
   "packages/server/src/service/materialization/serve/bind.spec.ts",
   "packages/server/src/service/materialization/serve/bindings.spec.ts",
   "packages/server/src/service/materialization/serve/join_rollup.spec.ts",
   "packages/server/src/service/materialization/serve/native_schema_notices.spec.ts",
   "packages/server/src/service/materialization/serve/query_path.spec.ts",
   "packages/server/src/service/materialization/serve/query_variants.spec.ts",
   "packages/server/src/service/materialization/serve/scope_facts.spec.ts",
   "packages/server/src/service/materialization/serve/serve_verdict.spec.ts",
   "packages/server/src/service/materialization/serve/served_copy.spec.ts",
   "packages/server/src/service/materialization/serve/served_copy_check_order.spec.ts",
   "packages/server/src/service/materialization/serve/served_copy_classification_ceiling.spec.ts",
   "packages/server/src/service/materialization/serve/served_copy_containment.spec.ts",
   "packages/server/src/service/materialization/serve/served_copy_lost_catalog.spec.ts",
   "packages/server/src/service/materialization/serve/served_copy_refinement_guard.spec.ts",
   "packages/server/src/service/materialization/serve/served_copy_refusal_codes.spec.ts",
   "packages/server/src/service/materialization/serve/served_copy_source_declines.spec.ts",
   "packages/server/src/service/materialization/serve/storage_bindings.spec.ts",
   "packages/server/src/service/materialization/serve/view_call.spec.ts",
   "packages/server/src/service/materialization/serve/view_variants.spec.ts",
   "packages/server/src/service/materialization/serve/view_variants_serve.spec.ts",
   "packages/server/src/service/materialization/stored_column_types.spec.ts",
   "packages/server/src/service/materialization/text/author_model.spec.ts",
   "packages/server/src/service/materialization/text/cross_connection_text.spec.ts",
   "packages/server/src/service/materialization/text/declaration_text_imports.spec.ts",
   "packages/server/src/service/materialization/text/kept_ancestors.spec.ts",
   "packages/server/src/service/materialization/text/kept_ancestors_declared.spec.ts",
   "packages/server/src/service/materialization/text/source_map_reach.spec.ts",
   "packages/server/src/service/materialization/tier_vocabulary.spec.ts",
   "packages/server/src/service/misplaced_join_note_identity.spec.ts",
   "packages/server/src/service/model_info.spec.ts",
   "packages/server/src/service/motly_route_tag.spec.ts",
   "packages/server/src/service/notebook_format.spec.ts",
   "packages/server/src/service/notebook_lint.spec.ts",
   "packages/server/src/service/package.spec.ts",
   "packages/server/src/service/probe_compiler.spec.ts",
   "packages/server/src/service/query_text.spec.ts",
   "packages/server/src/service/report_layout_parity.spec.ts",
   "packages/server/src/service/source_given_probe.spec.ts",
   "packages/server/src/service/source_namespace.spec.ts",
   "packages/server/src/service/sql_embed_gates.spec.ts",
   "packages/server/src/service/workload_shape.spec.ts",
   "packages/server/src/test_helpers/authorize_fixture.ts",
   "packages/server/src/test_helpers/incremental_test_harness.ts",
   "packages/server/src/test_helpers/materialization_fixture.ts",
   "packages/server/src/test_helpers/materialize_spec_models.ts",
   "packages/server/tests/component/filter_binding_guard.component.spec.ts",
   "packages/server/tests/component/filter_binding_guard_query_source.component.spec.ts",
   "packages/server/tests/component/notebook_serve.component.spec.ts",
   "packages/server/tests/component/r4/coverage_refusals.component.spec.ts",
   "packages/server/tests/component/r4/r4.component.spec.ts",
];
