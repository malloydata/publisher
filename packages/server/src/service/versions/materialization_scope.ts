// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "crypto";
import type {
   ManifestEntry,
   Materialization,
   ResourceRepository,
} from "../../storage/DatabaseInterface";

/**
 * Who owns the tables a published version's materializations build, decided
 * by the package's `materialization.scope`:
 *
 *  - `scope: "version"`: each version owns its tables. A self-assigned name
 *    gains the version as a suffix, reuse and the serve binding read only that
 *    version's runs, and archiving the version reclaims them.
 *  - `scope: "package"` (the default): the versions share the package's
 *    tables, under unchanged names. Each version binds only the tables its own
 *    definition built (the serve binding checks the content address).
 *
 * An unversioned package is neither: its runs are the package's, as they were
 * before versions existed.
 */

/** What these rules read off a loaded package. */
export interface ScopedPackage {
   getVersionId(): string | undefined;
   getPackageMetadata(): { scope?: string | null };
}

/**
 * The version whose own runs a loaded package builds and serves from: its
 * version, for a published version of a `scope: version` package; undefined
 * when it reads the package's shared runs (`scope: package`, or unversioned).
 */
export function ownedVersionOf(pkg: ScopedPackage): string | undefined {
   const version = pkg.getVersionId();
   if (version === undefined) return undefined;
   return pkg.getPackageMetadata().scope === "version" ? version : undefined;
}

/**
 * Whether a run built tables one version owns: a run of a `scope: version`
 * version. Never read as one of the package's shared runs.
 */
export function isVersionOwnedRun(
   m: Pick<Materialization, "version" | "metadata">,
): boolean {
   return m.version !== null && m.metadata?.scope === "version";
}

/**
 * Where a run records, before it builds anything, the shared tables it is
 * about to rebuild: an auto-run of a `scope: package` version rebuilds them in
 * place, one source at a time, under the names every version reads. Replaced,
 * with the rest of the run's metadata, when the run commits.
 */
export const REBUILDING_TABLES_KEY = "rebuildingTables";

/**
 * `entries` (the newest committed shared run's) minus every table a newer run
 * set out to rebuild without committing. Such a table may already hold what
 * that run's definition builds, and keeps holding it if the run failed, so
 * `entries` can no longer say what is in it. Dropped, the table serves live
 * for every version, and the next run rebuilds it rather than reusing it.
 */
export function withoutUnsettledRebuilds(
   entries: Record<string, ManifestEntry>,
   newerRuns: Pick<Materialization, "metadata">[],
): Record<string, ManifestEntry> {
   const unsettled = new Set<string>();
   for (const run of newerRuns) {
      const tables = run.metadata?.[REBUILDING_TABLES_KEY];
      if (!Array.isArray(tables)) continue;
      for (const table of tables) {
         if (typeof table === "string") unsettled.add(table);
      }
   }
   if (unsettled.size === 0) return entries;
   return Object.fromEntries(
      Object.entries(entries).filter(
         ([, entry]) => !unsettled.has(entry.physicalTableName),
      ),
   );
}

/**
 * The entries a package serves from, given its runs newest first: the newest
 * committed run among those it reads. `owned` names a version that reads only
 * its own runs; undefined reads the package's shared runs, less any table a
 * newer shared run is rebuilding (see withoutUnsettledRebuilds). `excludeId`
 * skips one run, the one asking.
 */
export function newestServingEntries(
   runs: Materialization[],
   owned: string | undefined,
   excludeId?: string,
): Record<string, ManifestEntry> {
   const newer: Materialization[] = [];
   for (const m of runs) {
      if (m.id === excludeId) continue;
      if (owned !== undefined ? m.version !== owned : isVersionOwnedRun(m)) {
         continue;
      }
      if (m.status === "MANIFEST_FILE_READY" && m.manifest?.entries) {
         return owned !== undefined
            ? m.manifest.entries
            : withoutUnsettledRebuilds(m.manifest.entries, newer);
      }
      newer.push(m);
   }
   return {};
}

/**
 * How a loaded package finds, in the store, the tables it serves from: the
 * newest committed run it reads (see newestServingEntries). `owned` names a
 * `scope: version` version, which reads only its own runs.
 */
export function storageBindingResolverFor(
   repository: Pick<ResourceRepository, "listMaterializations">,
   environmentId: string,
): (
   packageName: string,
   owned: string | undefined,
) => Promise<Record<string, ManifestEntry>> {
   return async (packageName, owned) =>
      newestServingEntries(
         await repository.listMaterializations(
            environmentId,
            packageName,
            owned !== undefined ? { version: owned } : undefined,
         ),
         owned,
      );
}

/**
 * The longest table segment a version-owned name may have: Postgres truncates
 * identifiers at 63 characters, and a build appends a 13-character staging
 * suffix to the name while it builds.
 */
const VERSIONED_TABLE_SEGMENT_MAX = 50;

/**
 * The suffix that makes a self-assigned table one version's own, under
 * `scope: version`: `__v1_2_3` for a release. A version with a pre-release or
 * build part also gets 8 hex of its hash, because its dots, dashes and plus
 * signs cannot stand in an identifier, and the underscored form alone could
 * name two versions (`1.0.0-rc.1` and `1.0.0-rc-1`).
 */
export function versionTableSuffix(version: string): string {
   const release = /^(\d+)\.(\d+)\.(\d+)/.exec(version);
   const base = release
      ? `${release[1]}_${release[2]}_${release[3]}`
      : version.replace(/[^A-Za-z0-9]/g, "_");
   if (release && release[0] === version) return `__v${base}`;
   return `__v${base}_${shortHash(version)}`;
}

/**
 * A self-assigned name made one version's own: `base` with `suffix` on its
 * table segment (the part after the last dot), kept within
 * VERSIONED_TABLE_SEGMENT_MAX so a dialect that truncates long identifiers
 * never folds two versions' tables into one. A segment too long for the
 * suffix is cut and given 8 hex of its hash, so distinct names stay distinct.
 */
export function versionedTableName(base: string, suffix: string): string {
   const dot = base.lastIndexOf(".");
   const prefix = base.slice(0, dot + 1);
   const segment = base.slice(dot + 1);
   if (segment.length + suffix.length <= VERSIONED_TABLE_SEGMENT_MAX) {
      return `${prefix}${segment}${suffix}`;
   }
   const digest = shortHash(segment);
   const room = VERSIONED_TABLE_SEGMENT_MAX - suffix.length - digest.length - 1;
   return `${prefix}${segment.slice(0, Math.max(room, 1))}_${digest}${suffix}`;
}

function shortHash(text: string): string {
   return createHash("sha256").update(text).digest("hex").substring(0, 8);
}
