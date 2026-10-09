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
 * Who reads a package's runs: a published version (`versionId`), which under
 * `scope: version` reads only its own runs (`owned`), or the package's
 * unversioned slot (neither).
 */
export interface ServingReader {
   versionId?: string;
   owned?: string;
}

/** The reader a loaded package is. */
export function servingReaderOf(pkg: ScopedPackage): ServingReader {
   const versionId = pkg.getVersionId();
   if (versionId === undefined) return {};
   return { versionId, owned: ownedVersionOf(pkg) };
}

/**
 * Where a run records the tables of other runs it has made stale: an auto-run
 * of a `scope: package` version marks, on every older run, the entries for the
 * shared tables it is about to rebuild, before it builds them. Whatever then
 * happens to this run (it commits, fails, or its record is deleted), those
 * tables no longer hold what the older entries say, so they are never read
 * again.
 */
export const SUPERSEDED_TABLES_KEY = "supersededTables";

/**
 * Where a published version's run records the tables it writes, once they are
 * known: at its start for a run with instructions, before it builds for an
 * auto-run, whether its tables are shared or its version's own. Two active
 * runs never write one table.
 */
export const WRITES_TABLES_KEY = "writesTables";

/** A metadata list of table names, or empty. */
export function tableNamesIn(
   metadata: Record<string, unknown> | null,
   key: string,
): string[] {
   const tables = metadata?.[key];
   return Array.isArray(tables)
      ? tables.filter((t): t is string => typeof t === "string")
      : [];
}

/** Whether `reader` serves from what run `m` built. */
function readsRun(m: Materialization, reader: ServingReader): boolean {
   if (reader.owned !== undefined) return m.version === reader.owned;
   if (isVersionOwnedRun(m)) return false;
   // A run with instructions built the tables its caller named for its own
   // version: another version never serves them, nor counts it as the newest.
   return !(
      reader.versionId !== undefined &&
      m.metadata?.mode === "orchestrated" &&
      m.version !== null &&
      m.version !== reader.versionId
   );
}

/** When a committed run committed; runs are ordered by it, not by creation. */
function committedAt(m: Materialization): number {
   return (m.completedAt ?? m.createdAt).getTime();
}

/**
 * The entries a package serves from: those of the newest committed run among
 * the runs `reader` reads (see readsRun), less the tables a later run has
 * superseded (see SUPERSEDED_TABLES_KEY). `excludeId` skips one run, the one
 * asking.
 */
export function newestServingEntries(
   runs: Materialization[],
   reader: ServingReader,
   excludeId?: string,
): Record<string, ManifestEntry> {
   let newest: Materialization | undefined;
   for (const m of runs) {
      if (m.id === excludeId || !readsRun(m, reader)) continue;
      if (m.status !== "MANIFEST_FILE_READY" || !m.manifest?.entries) continue;
      if (!newest || committedAt(m) > committedAt(newest)) newest = m;
   }
   if (!newest?.manifest?.entries) return {};
   const superseded = new Set(
      tableNamesIn(newest.metadata, SUPERSEDED_TABLES_KEY).map(tableIdentity),
   );
   if (superseded.size === 0) return newest.manifest.entries;
   return Object.fromEntries(
      Object.entries(newest.manifest.entries).filter(
         ([, entry]) => !superseded.has(tableIdentity(entry.physicalTableName)),
      ),
   );
}

/**
 * What a physical table name names, for telling whether two runs write the
 * same table: its last segment, unquoted, without letter case. Two names a
 * warehouse may read as one table (`summary` and `SUMMARY` in DuckDB, or
 * `main.summary` and `summary`) get one identity. It can also give two
 * distinct tables one (`a.t` and `b.t`), which only ever makes a run refuse
 * or a version serve live: the safe direction.
 */
export function tableIdentity(physicalTableName: string): string {
   const segments = physicalTableName.split(".");
   return segments[segments.length - 1].replace(/["`[\]]/g, "").toLowerCase();
}

/**
 * How a loaded package finds, in the store, the tables it serves from (see
 * newestServingEntries).
 */
export function storageBindingResolverFor(
   repository: Pick<ResourceRepository, "listMaterializations">,
   environmentId: string,
): (
   packageName: string,
   reader: ServingReader,
) => Promise<Record<string, ManifestEntry>> {
   return async (packageName, reader) =>
      newestServingEntries(
         await repository.listMaterializations(
            environmentId,
            packageName,
            reader.owned !== undefined ? { version: reader.owned } : undefined,
         ),
         reader,
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
