// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A drive-less `file:///…` URL in a spec or test helper (the lint rule
// `publisher/no-driveless-file-url`, eslint.config.mjs). On Windows such a URL
// names no path: `fileURLToPath` throws and a cache keyed by path misses, so a
// spec that turns one into a path fails there alone. Build the URL with
// src/test_helpers/test_file_url.ts instead.
//
// DRIVELESS_FILE_URL_BASELINE is the files that spelled one when the rule was
// added, server-relative. What is left is opaque keys never read as a path
// (an InMemoryURLReader's map, a source ID, a location compared as a string),
// so it is harmless today, but the list only shrinks:
// src/driveless_file_urls.spec.ts fails on an entry whose file no longer
// spells one (delete it) and on a file that spells one and is not listed (use
// the helper; do not add it).

/**
 * A string's text, or a template literal's text before its first
 * substitution, that begins a drive-less file URL: `file:///` followed by
 * anything but a drive letter (`file:///C:/…`). A bare `file:///` counts only
 * when a substitution follows it (`file:///${name}`), since alone it is a
 * prefix to compare with, not a location.
 */
export function isDrivelessFileURL(head, substituted) {
   if (!head.startsWith("file:///")) return false;
   const rest = head.slice("file:///".length);
   if (rest === "") return substituted;
   return !/^[A-Za-z]:/.test(rest);
}

/** Server-relative files allowed to keep their drive-less file URLs. */
export const DRIVELESS_FILE_URL_BASELINE = [
   "src/build_plan_omitted_sources_contract.spec.ts",
   "src/mcp/tools/compile_tool.spec.ts",
   "src/package_load/job_files.spec.ts",
   "src/service/annotation_near_miss.spec.ts",
   "src/service/annotation_ownership.spec.ts",
   "src/service/annotation_reader.spec.ts",
   "src/service/annotations.spec.ts",
   "src/service/authorize_grammar.spec.ts",
   "src/service/build_targets_given_binding.spec.ts",
   "src/service/caller_joins.spec.ts",
   "src/service/compile_document.spec.ts",
   "src/service/connection_config.spec.ts",
   "src/service/drill_probe.spec.ts",
   "src/service/entities/field_helpers.spec.ts",
   "src/service/entities/model_text.spec.ts",
   "src/service/entities/source_info_serializer.spec.ts",
   "src/service/environment_write_model_file.spec.ts",
   "src/service/exports_probe.spec.ts",
   "src/service/filter_binding_guard.spec.ts",
   "src/service/gate_registry_walk.spec.ts",
   "src/service/gate_walk_differential.spec.ts",
   "src/service/given_required.spec.ts",
   "src/service/governance/ir/declared_notes.spec.ts",
   "src/service/governance/ir/source_graph.spec.ts",
   "src/service/governance/ir/source_reads.spec.ts",
   "src/service/malloy_annotation_invariants.spec.ts",
   "src/service/malloy_text.spec.ts",
   "src/service/managed_catalog_scope_postgres.spec.ts",
   "src/service/materialization/dialect_capabilities_characterization.spec.ts",
   "src/service/materialization/domain/same_catalog_build.spec.ts",
   "src/service/materialization/domain/upstream_schemas.spec.ts",
   "src/service/materialization/plan/addressing_destination.spec.ts",
   "src/service/materialization/plan/build_file_generator.spec.ts",
   "src/service/materialization/plan/build_plan_compile.spec.ts",
   "src/service/materialization/plan/build_plan_gate_classification.spec.ts",
   "src/service/materialization/plan/build_plan_omitted_sources.spec.ts",
   "src/service/materialization/plan/declaration.spec.ts",
   "src/service/materialization/plan/extension_chain_verdict.spec.ts",
   "src/service/materialization/plan/freshness_window_message.spec.ts",
   "src/service/materialization/plan/materialization_eligibility.spec.ts",
   "src/service/materialization/plan/materialize_build_copy.spec.ts",
   "src/service/materialization/plan/materialize_plan.spec.ts",
   "src/service/materialization/plan/materialize_read_time.spec.ts",
   "src/service/materialization/plan/materialize_stored_chain.spec.ts",
   "src/service/materialization/plan/persisted_view.spec.ts",
   "src/service/materialization/plan/preaggregation_validation.spec.ts",
   "src/service/materialization/plan/refusal_findings_characterization.spec.ts",
   "src/service/materialization/plan/rolling_window.spec.ts",
   "src/service/materialization/plan/rollup_plan.spec.ts",
   "src/service/materialization/plan/twin_addresses.spec.ts",
   "src/service/materialization/plan/upstream_schema_address.spec.ts",
   "src/service/materialization/plan/v1_rollup_plan.spec.ts",
   "src/service/materialization/run/incremental_recast.spec.ts",
   "src/service/materialization/run/incremental_storage_rename.spec.ts",
   "src/service/materialization/run/key_check_null_rule.spec.ts",
   "src/service/materialization/run/self_table_naming.spec.ts",
   "src/service/materialization/run/storage_extension_build.spec.ts",
   "src/service/materialization/run/table_ownership.spec.ts",
   "src/service/materialization/serve/native_schema_notices.spec.ts",
   "src/service/materialization/serve/query_path.spec.ts",
   "src/service/materialization/serve/served_copy.spec.ts",
   "src/service/materialization/serve/served_copy_classification_ceiling.spec.ts",
   "src/service/materialization/serve/served_copy_containment.spec.ts",
   "src/service/materialization/serve/served_copy_lost_catalog.spec.ts",
   "src/service/materialization/serve/served_copy_refinement_guard.spec.ts",
   "src/service/materialization/serve/view_call.spec.ts",
   "src/service/materialization/serve/view_variants.spec.ts",
   "src/service/materialization/stored_column_types.spec.ts",
   "src/service/materialization/text/cross_connection_text.spec.ts",
   "src/service/materialization/text/declaration_text_imports.spec.ts",
   "src/service/materialization/text/kept_ancestors.spec.ts",
   "src/service/materialization/text/kept_ancestors_declared.spec.ts",
   "src/service/misplaced_join_note_identity.spec.ts",
   "src/service/model_info.spec.ts",
   "src/service/motly_route_tag.spec.ts",
   "src/service/notebook_format.spec.ts",
   "src/service/notebook_lint.spec.ts",
   "src/service/probe_compiler.spec.ts",
   "src/service/query_text.spec.ts",
   "src/service/report_layout_parity.spec.ts",
   "src/service/source_given_probe.spec.ts",
   "src/service/source_namespace.spec.ts",
   "src/service/sql_embed_gates.spec.ts",
   "src/service/workload_shape.spec.ts",
   "src/test_helpers/authorize_fixture.ts",
   "src/test_helpers/incremental_test_harness.ts",
   "src/test_helpers/materialization_fixture.ts",
   "tests/component/filter_binding_guard.component.spec.ts",
   "tests/component/filter_binding_guard_query_source.component.spec.ts",
   "tests/component/notebook_serve.component.spec.ts",
   "tests/component/r4/coverage_refusals.component.spec.ts",
   "tests/component/r4/r4.component.spec.ts",
];
