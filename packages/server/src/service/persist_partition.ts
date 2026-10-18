// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { PersistSource } from "@malloydata/malloy";

/**
 * The `partition=` key on a `#@ persist` tag: the columns a storage build lays
 * the destination table out by.
 *
 * A LAYOUT choice that carries no isolation: every stripped dynamic term is
 * re-applied on read whether or not its column is partitioned, so omitting the
 * caller's scoping column costs a full scan, never a leak.
 *
 * That is why the list is author-chosen rather than derived from the stripped
 * terms' equality columns: forcing those would create a partition per user for
 * a source whose rows many users share.
 */
type PartitionRefusal =
   | "partition_without_storage"
   | "partition_column_unknown"
   | "partition_column_not_public";

type PartitionResolution =
   | { ok: true; columns: string[] }
   | { ok: false; reason: PartitionRefusal; detail: string };

/**
 * Split a `partition=` value into column names. Comma-separated and ordered —
 * DuckLake nests partition directories in the order given, so `"org_id, day"`
 * and `"day, org_id"` are different layouts and the author's order is kept.
 * Empty entries are dropped, so a trailing comma is not an error.
 */
export function parsePartitionValue(raw: string): string[] {
   return raw
      .split(",")
      .map((name) => name.trim())
      .filter((name) => name.length > 0);
}

/**
 * Resolve a source's declared `partition=` against the relation the build will
 * actually write, or refuse naming the column.
 *
 * The build writes only the source's PUBLIC projection (`projectToPublicColumns`),
 * so a column outside it does not exist in the table being partitioned. Refusing
 * here replaces an opaque mid-build SQL error with a publish-time message.
 *
 * `partition=` without `storage=` is refused, not ignored: a colocated build's
 * layout is the customer warehouse's own DDL, so silently accepting the key
 * would mislead.
 */
export function resolvePartitionColumns(
   persistSource: PersistSource,
   annotationFields: Record<string, string>,
): PartitionResolution {
   const raw = annotationFields.partition;
   if (raw === undefined) return { ok: true, columns: [] };

   const columns = parsePartitionValue(raw);
   if (columns.length === 0) return { ok: true, columns: [] };

   if (!annotationFields.storage) {
      return {
         ok: false,
         reason: "partition_without_storage",
         detail:
            `'partition=' is declared without 'storage='. A colocated ` +
            `'#@ persist' writes into the source's own warehouse, where the ` +
            `table layout is that warehouse's own DDL — the publisher does ` +
            `not lay it out. Add 'storage=' to build into a storage ` +
            `destination, or drop 'partition='`,
      };
   }

   const { publicNames, declaredNames } = columnSurface(persistSource);
   for (const column of columns) {
      if (publicNames.has(column)) continue;
      if (declaredNames.has(column)) {
         return {
            ok: false,
            reason: "partition_column_not_public",
            detail:
               `'partition=' names '${column}', which the source hides. The ` +
               `stored table is narrowed to the source's public columns, so a ` +
               `hidden one is not there to partition by. Make it public, or ` +
               `partition by a column that is`,
         };
      }
      return {
         ok: false,
         reason: "partition_column_unknown",
         detail:
            `'partition=' names '${column}', which is not a column of the ` +
            `source (a column removed with 'except:' is not one either). ` +
            `Partition by a column the source projects`,
      };
   }
   return { ok: true, columns };
}

/**
 * The source's public column names and the full set it declares.
 *
 * Reads `SourceDef.fields` rather than `_explore.intrinsicFields` because it
 * needs hidden columns too (present with a non-public `accessModifier`) to tell
 * "hidden" from "unknown". An `except:`-removed column is absent, so reports as
 * unknown.
 *
 * Unreadable IR yields empty sets, refusing every column: fail closed, since a
 * refused build is only a cost while DDL against unseen columns fails mid-build.
 */
function columnSurface(persistSource: PersistSource): {
   publicNames: Set<string>;
   declaredNames: Set<string>;
} {
   const publicNames = new Set<string>();
   const declaredNames = new Set<string>();
   try {
      const def = persistSource._sourceDef as unknown as {
         fields?: unknown[];
      };
      for (const field of def?.fields ?? []) {
         if (field === null || typeof field !== "object") continue;
         const f = field as Record<string, unknown>;
         // A join is not a column; its own fields are reached through it.
         if (f.join !== undefined) continue;
         const name = typeof f.as === "string" ? f.as : f.name;
         if (typeof name !== "string" || name.length === 0) continue;
         declaredNames.add(name);
         const access = f.accessModifier;
         if (access == null || access === "public") publicNames.add(name);
      }
   } catch {
      return { publicNames: new Set(), declaredNames: new Set() };
   }
   return { publicNames, declaredNames };
}
