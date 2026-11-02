// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every strict-at-publish check joined into one 400 message, or undefined when
 * the package is publishable: explores, persistence policy, incremental
 * refresh, pre-aggregation, and persist-target collisions (only under
 * `PERSIST_COLLISION_ENFORCE`, so a latent collision does not block a routine
 * re-publish). At startup/reload these only warn (fail-safe), except the
 * incremental-refresh and pre-aggregation gates, which fail the load too.
 */
export function formatPublishRejections(
   pkg: {
      formatInvalidExplores(exploresOverride?: string[]): string;
      formatInvalidPersistencePolicy(): string;
      formatInvalidIncrementalPolicy(): string;
      formatInvalidPreaggregatePolicy(): string;
      formatPersistenceCollisionRejections(): string;
   },
   exploresOverride?: string[],
): string | undefined {
   const message = [
      pkg.formatInvalidExplores(exploresOverride),
      pkg.formatInvalidPersistencePolicy(),
      pkg.formatInvalidIncrementalPolicy(),
      pkg.formatInvalidPreaggregatePolicy(),
      pkg.formatPersistenceCollisionRejections(),
   ]
      .filter(Boolean)
      .join("\n");
   return message || undefined;
}
