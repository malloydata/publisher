// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every strict-at-publish check joined into one 400 message, or undefined when
 * the package is publishable: explores, persistence policy, `#@` lines that
 * do not parse, incremental
 * refresh, pre-aggregation, and persist-target collisions (unless
 * `PERSIST_COLLISION_ENFORCE=false` opts out, for a deployment with latent
 * collisions it cannot fix yet). At startup/reload these only warn (fail-safe), except the
 * incremental-refresh and pre-aggregation gates, which fail the load too.
 */
export function formatPublishRejections(
   pkg: {
      formatInvalidExplores(exploresOverride?: string[]): string;
      formatInvalidPersistencePolicy(): string;
      formatUnparseableAnnotations(): string;
      formatInvalidIncrementalPolicy(): string;
      formatInvalidPreaggregatePolicy(): string;
      formatPersistenceCollisionRejections(): string;
   },
   exploresOverride?: string[],
): string | undefined {
   const message = [
      pkg.formatInvalidExplores(exploresOverride),
      pkg.formatInvalidPersistencePolicy(),
      pkg.formatUnparseableAnnotations(),
      pkg.formatInvalidIncrementalPolicy(),
      pkg.formatInvalidPreaggregatePolicy(),
      pkg.formatPersistenceCollisionRejections(),
   ]
      .filter(Boolean)
      .join("\n");
   return message || undefined;
}
