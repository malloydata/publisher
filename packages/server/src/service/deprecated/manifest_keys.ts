// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * DEPRECATED: the publisher.json keys a package still declares: `explores`
 * and `queryableSources`, which still work, and `agents`, which an earlier
 * release read and this one does not. The package-load worker reads them off
 * the raw manifest (the resolved `explores` is also filled in from
 * `index.malloy`, and the resolved `queryableSources` always has a value, so
 * neither says whether the key was written) and the main thread counts them
 * once per load or reload. Dropping a key is deleting its entry here.
 *
 * Two findings say a package's content is no longer read, each with a stable
 * `code` that names the package after the upgrade: the `agents` key, and a
 * root `skills/` directory no declared skill set is at or under. The usage
 * metric counts loads, not packages.
 */

import { recordDeprecatedUsage } from "../../deprecated_usage_metrics";
import type { ApiPackageWarning } from "../classification";

export type DeprecatedManifestKey = "explores" | "queryableSources" | "agents";

const FEATURE = {
   explores: "explores",
   queryableSources: "queryable_sources",
   agents: "agents",
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

export const PACKAGE_AGENTS_NOT_READ = "package-agents-not-read";
export const PACKAGE_SKILLS_DIR_NOT_READ = "package-skills-dir-not-read";

/** The finding for a manifest that declares `agents` at any value; none when it does not. */
export function agentsNotReadFindings(parsed: {
   agents?: unknown;
}): ApiPackageWarning[] {
   return parsed.agents === undefined
      ? []
      : [
           {
              code: PACKAGE_AGENTS_NOT_READ,
              message:
                 '"agents" is no longer read. Fix: declare "workflows" instead; the "Migrating from agents and a root skills/" section of the packages guide shows how each agent maps onto one',
              severity: "warn",
           },
        ];
}

export const SKILLS_DIR_NOT_READ: ApiPackageWarning = {
   code: PACKAGE_SKILLS_DIR_NOT_READ,
   severity: "warn",
   message:
      'skills/ is no longer read, so none of its skills is served. Fix: add "skillSets": { "default": { "path": "skills" } } to publisher.json, listing in its "overrides" each skill that replaces a bundled one, and put anything a skill must not serve in a .skillignore; the "Migrating from agents and a root skills/" section of the packages guide has the details',
};

/** Count one load or reload of a package whose root `skills/` nothing reads. */
export function recordSkillsDirNotRead(): void {
   recordDeprecatedUsage("skills_dir");
}
