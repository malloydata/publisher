// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Materialization's one public entry: what the rest of the server may import
 * from this module, besides the shared vocabulary in `domain/`.
 *
 * The package facade (`PackageMaterialization`), the run's services
 * (`MaterializationService`, `Reclaimer`, `MaterializationScheduler`) and the
 * host port's types. Callers outside the module still reach past it into the
 * layers; each is listed in the import-direction allowlist with the
 * step that moves it here, and the list only shrinks.
 *
 * Re-exports only.
 */

export { PackageMaterialization } from "./package_materialization";
export { withNameProblems } from "./plan/load_gate_text";
export { loadVerdictOfPlan } from "./package_warnings";
export { removeStaleBuildDirs } from "./run/materialization_build_session";
export { MaterializationService } from "./run/materialization_service";
export { keptTablesResolverFor, Reclaimer } from "./run/reclaimer";
export { isUsableTableName } from "./run/manifest_loader";
export { MaterializationScheduler } from "./run/materialization_scheduler";
export { readUpstreamSchemas } from "./run/upstream_schema_read";
export type * from "./host";
export type { SynthesizedRuntimeFactory } from "./text/generated_files";
