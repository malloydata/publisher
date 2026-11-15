// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The readable name Publisher gives a table it names itself:
 *
 *    [<container>.]<pkg>__<file>__<source>[__v<ver>]__p<8 hex>
 *
 * The prefix, `<pkg>__<file>__<source>[__v<ver>]`, is the control plane's
 * too: the same segments, sanitizing, budgets and truncation order, so a
 * hosted table and a standalone one read alike in a warehouse listing. Only
 * the tail differs. A hosted name ends `__g<seq>__<12 hex>`; a standalone one
 * ends `__p<8 hex>`, the first 8 hex of a hash over the table's identity
 * (environment, package, model path, source, version), so two packages, or
 * two environments, on one schema never write one table, and a name
 * Publisher chose still ends the one way an author's `name=` may not
 * (table_ownership.ts).
 *
 * Uniqueness never rests on the readable parts, which are sanitized and may
 * be cut: it rests on the hash.
 *
 * Pure.
 */

import { createHash } from "crypto";

/**
 * Each dialect's leaf budget: its identifier limit less the 13 characters a
 * build appends for its staging and aside copies, capped at 120 for
 * readability. Keyed by Malloy dialect name.
 *
 *  - Postgres (and Redshift, which connects as `postgres`), MySQL, Trino and
 *    Presto (whose limit is the catalog's behind it, 63 or 64 over Postgres
 *    or MySQL), and any dialect not listed: 50.
 *  - SQL Server: 128 less 13.
 *  - DuckDB, Snowflake, BigQuery (`standardsql`) and Databricks: 120.
 *
 * A table in a storage destination (DuckLake) takes 120 whatever its source's
 * dialect.
 */
const LEAF_BUDGETS: ReadonlyMap<string, number> = new Map([
   ["duckdb", 120],
   ["snowflake", 120],
   ["standardsql", 120],
   ["databricks", 120],
   ["sqlserver", 115],
   ["tsql", 115],
]);

/** The budget of a dialect {@link LEAF_BUDGETS} does not list. */
const DEFAULT_LEAF_BUDGET = 50;

/** A storage destination's budget. */
const DESTINATION_LEAF_BUDGET = 120;

/** The floors of the truncation steps (see {@link fitReadable}). */
const PKG_FILE_FLOOR = 6;
const SOURCE_FLOOR = 8;
const LAST_FLOOR = 4;

/** The longest a non-semantic version's segment may be. */
const VERSION_CAP = 10;

/** The extensions a model path's base name loses, a companion's included. */
const MODEL_EXTENSIONS = [
   ".malloynb",
   ".malloy",
   ".persist-view",
   ".materialize",
];

/** The suffix a persisted view's generated build source carries. */
const VIEW_BUILD_SUFFIX = "__view";

/** The leaf budget of a table built with `dialect`, or in a storage destination. */
export function leafBudget(
   dialect: string | undefined,
   inDestination: boolean,
): number {
   if (inDestination) return DESTINATION_LEAF_BUDGET;
   return (
      LEAF_BUDGETS.get((dialect ?? "").toLowerCase()) ?? DEFAULT_LEAF_BUDGET
   );
}

/** `s` without leading and trailing underscores. */
function trimUnderscores(s: string): string {
   return s.replace(/^_+|_+$/g, "");
}

/**
 * One readable segment: lowercase; every character outside `[a-z0-9]`
 * becomes `_`; runs of `_` collapse to one; leading and trailing `_` are
 * trimmed; empty becomes `x`; a leading digit takes a `t`. No segment holds
 * `__`, so `__` is a true separator.
 */
export function readableSegment(raw: string | undefined): string {
   const s = trimUnderscores(
      (raw ?? "")
         .toLowerCase()
         .replace(/[^a-z0-9]/g, "_")
         .replace(/_+/g, "_"),
   );
   if (s === "") return "x";
   return /^[0-9]/.test(s) ? `t${s}` : s;
}

/**
 * The `file` segment of a model path: its base name, with no directory and no
 * extension. A generated companion (`orders.malloy.persist-view.malloy`) is
 * the author's file.
 */
export function fileSegment(modelPath: string | undefined): string {
   let base = modelPath ?? "";
   base = base.slice(
      Math.max(base.lastIndexOf("/"), base.lastIndexOf("\\")) + 1,
   );
   let previous: string;
   do {
      previous = base;
      for (const ext of MODEL_EXTENSIONS) {
         if (base.endsWith(ext)) base = base.slice(0, base.length - ext.length);
      }
   } while (base !== previous);
   return readableSegment(base);
}

/**
 * The `v<ver>` segment of a `scope: version` table: `v<maj>_<min>_<pat>` for
 * a semantic version (a prerelease or build suffix is not shown; the hash
 * tells them apart), else the sanitized version cut to 10.
 */
export function readableVersion(versionId: string | undefined): string {
   const semver = /^v?(\d+)\.(\d+)\.(\d+)/.exec(versionId ?? "");
   if (semver) return `v${semver[1]}_${semver[2]}_${semver[3]}`;
   let s = readableSegment(versionId);
   if (s.startsWith("t") && /^[0-9]/.test(versionId ?? "")) s = s.slice(1);
   if (s.length > VERSION_CAP) s = s.slice(0, VERSION_CAP).replace(/_+$/, "");
   return `v${s}`;
}

/** One character off the end of `part`, and any `_` that leaves trailing. */
function cut(part: string): string {
   const c = part.slice(0, -1).replace(/_+$/, "");
   return c === "" ? part.slice(0, 1) : c;
}

function joinedLength(parts: (string | null)[]): number {
   const present = parts.filter((p): p is string => p !== null);
   return (
      present.reduce((n, p) => n + p.length, 0) +
      Math.max(0, present.length - 1) * 2
   );
}

/** Cut the longer of `pkg` and `file` until the parts fit or both reach `floor`. */
function cutPkgFile(
   parts: [string | null, string | null, string],
   room: number,
   floor: number,
): void {
   while (joinedLength(parts) > room) {
      const pkg = parts[0] ?? "";
      const file = parts[1] ?? "";
      const pkgCuttable = pkg.length > floor;
      const fileCuttable = file.length > floor;
      if (!pkgCuttable && !fileCuttable) return;
      if (pkgCuttable && (!fileCuttable || pkg.length >= file.length)) {
         parts[0] = cut(pkg);
      } else {
         parts[1] = cut(file);
      }
   }
}

/**
 * `pkg__file__source` cut to at most `room` characters, stopping as soon as
 * it fits: (1) the longer of `pkg` and `file`, one character at a time, never
 * below 6 (a tie cuts `pkg`); (2) `source`, never below 8; (3) `pkg` and
 * `file` to 4, longest first; (4) drop `file`, then `pkg`. A trailing `_` a
 * cut leaves is trimmed. Deterministic.
 */
export function fitReadable(
   pkg: string,
   file: string,
   source: string,
   room: number,
): string {
   const parts: [string | null, string | null, string] = [pkg, file, source];
   cutPkgFile(parts, room, PKG_FILE_FLOOR);
   while (joinedLength(parts) > room && parts[2].length > SOURCE_FLOOR) {
      parts[2] = cut(parts[2]);
   }
   cutPkgFile(parts, room, LAST_FLOOR);
   if (joinedLength(parts) > room) parts[1] = null;
   if (joinedLength(parts) > room) parts[0] = null;
   return parts.filter((p): p is string => p !== null).join("__");
}

/**
 * The source a table's name reads: a rollup is `<base>_by_<grain dims>` (the
 * hash tells grains apart, so no digest), a persisted view's build source
 * (`<source>__<view>__view`) is `<source>__<view>`, which reads as
 * `<source>_<view>` once sanitized, and anything else its own name.
 */
export function sourceLabel(
   name: string,
   rollup?: { baseSourceName: string; grainDimensions: readonly string[] },
): string {
   if (rollup) {
      return `${rollup.baseSourceName}_by_${rollup.grainDimensions.join("_")}`;
   }
   return name.endsWith(VIEW_BUILD_SUFFIX)
      ? name.slice(0, name.length - VIEW_BUILD_SUFFIX.length)
      : name;
}

/** What one table Publisher names is for; its name's hash is over all of it. */
export interface TableIdentity {
   environmentName: string;
   packageName: string;
   /** The declaring model's package-relative path, with its extension. */
   modelPath: string;
   /** The planned source's own name (a rollup's or view's build source's). */
   sourceName: string;
   /** The owning version, for a `scope: version` table only. */
   version?: string;
}

/** Whether the table `a` names comes before `b` among the sources sharing one. */
export function namesBefore(
   a: { modelPath: string; label: string },
   b: { modelPath: string; label: string },
): boolean {
   if (a.modelPath !== b.modelPath) return a.modelPath < b.modelPath;
   return a.label < b.label;
}

/**
 * The readable name of the table `identity` names, after the source `label`
 * reads ({@link sourceLabel}): `container`, if any, then
 * `<pkg>__<file>__<source>` fitted to the dialect's budget, `__v<ver>` for a
 * `scope: version` table, and `__p` with 8 hex of the identity's hash.
 */
export function readableTableName(args: {
   identity: TableIdentity;
   label: string;
   container?: string;
   dialect: string | undefined;
   inDestination: boolean;
}): string {
   const { identity } = args;
   const hash = createHash("sha256")
      .update(
         JSON.stringify([
            "publisher-table/1",
            identity.environmentName,
            identity.packageName,
            identity.modelPath,
            identity.sourceName,
            identity.version ?? null,
         ]),
      )
      .digest("hex")
      .slice(0, 8);
   const tail =
      (identity.version !== undefined
         ? `__${readableVersion(identity.version)}`
         : "") + `__p${hash}`;
   const room = Math.max(
      1,
      leafBudget(args.dialect, args.inDestination) - tail.length,
   );
   const readable = fitReadable(
      readableSegment(identity.packageName),
      fileSegment(identity.modelPath),
      readableSegment(args.label),
      room,
   );
   return `${args.container ? `${args.container}.` : ""}${readable}${tail}`;
}
