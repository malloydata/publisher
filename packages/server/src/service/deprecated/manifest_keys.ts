// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: the publisher.json keys a package still declares, `explores`
 * and `queryableSources`. The package-load worker reads them off the raw
 * manifest (the resolved `explores` is also filled in from `index.malloy`, and
 * the resolved `queryableSources` always has a value, so neither says whether
 * the key was written) and the main thread counts them once per load or
 * reload. Dropping a key is deleting its entry here.
 */

import { recordDeprecatedUsage } from "../../deprecated_usage_metrics";

export type DeprecatedManifestKey = "explores" | "queryableSources";

const FEATURE = {
   explores: "explores",
   queryableSources: "queryable_sources",
} as const satisfies Record<DeprecatedManifestKey, string>;

/** Which deprecated keys a parsed publisher.json declares, at any value. */
export function declaredDeprecatedManifestKeys(
   parsed: Partial<Record<DeprecatedManifestKey, unknown>>,
): DeprecatedManifestKey[] {
   return (Object.keys(FEATURE) as DeprecatedManifestKey[]).filter(
      (key) => parsed[key] !== undefined,
   );
}

/** Count one use of each deprecated key a loaded package declares. */
export function recordDeprecatedManifestKeys(
   keys: readonly DeprecatedManifestKey[] | undefined,
): void {
   for (const key of keys ?? []) recordDeprecatedUsage(FEATURE[key]);
}
