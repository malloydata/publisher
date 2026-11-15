// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every strict-at-publish check joined into one 400 message, or undefined when
 * the package is publishable: explores, the package manifest's persistence
 * policy, invalid materialization annotations (which include persist-target
 * collisions unless `PERSIST_COLLISION_ENFORCE=false` opts out), incremental
 * refresh and pre-aggregation. At startup/reload the explores and manifest
 * policy only warn (fail-safe); the rest fail the load too (package_load_gates).
 */
export function formatPublishRejections(
   pkg: {
      formatInvalidExplores(exploresOverride?: string[]): string;
      formatInvalidPersistencePolicy(): string;
      formatInvalidAnnotations(): string;
      formatInvalidIncrementalPolicy(): string;
      formatInvalidPreaggregatePolicy(): string;
   },
   exploresOverride?: string[],
): string | undefined {
   const message = [
      pkg.formatInvalidExplores(exploresOverride),
      pkg.formatInvalidPersistencePolicy(),
      pkg.formatInvalidAnnotations(),
      pkg.formatInvalidIncrementalPolicy(),
      pkg.formatInvalidPreaggregatePolicy(),
   ]
      .filter(Boolean)
      .join("\n");
   return message || undefined;
}
