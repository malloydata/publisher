// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every strict-at-publish check joined into one 400 message, or undefined when
 * the package is publishable: explores, the package manifest's persistence
 * policy, and the load gate's text (every invalid materialization annotation,
 * persist-target collisions unless `PERSIST_COLLISION_ENFORCE=false` opts out,
 * invalid incremental refresh and pre-aggregation). At startup/reload the explores and manifest
 * policy only warn (fail-safe); the rest fail the load too (package_load_gates).
 */
export function formatPublishRejections(
   pkg: {
      formatInvalidExplores(exploresOverride?: string[]): string;
      formatInvalidPersistencePolicy(): string;
      formatLoadGateText(): string;
   },
   exploresOverride?: string[],
): string | undefined {
   const message = [
      pkg.formatInvalidExplores(exploresOverride),
      pkg.formatInvalidPersistencePolicy(),
      pkg.formatLoadGateText(),
   ]
      .filter(Boolean)
      .join("\n");
   return message || undefined;
}
