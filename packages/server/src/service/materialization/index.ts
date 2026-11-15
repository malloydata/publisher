// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Materialization's one public entry: what the rest of the server may import
 * from this module, besides the shared vocabulary in `domain/`.
 *
 * The package facade (`PackageMaterialization`), the run's services
 * (`MaterializationService`, `Reclaimer`, `MaterializationScheduler`) and the
 * host port's types. Callers outside the module still reach past it into the
 * layers; each is listed in the import-direction allowlist (R10) with the
 * step that moves it here, and the list only shrinks.
 *
 * Re-exports only.
 */

export { PackageMaterialization } from "./package_materialization";
export { MaterializationService } from "./run/materialization_service";
export { Reclaimer } from "./run/reclaimer";
export { MaterializationScheduler } from "./run/materialization_scheduler";
export type * from "./host";
