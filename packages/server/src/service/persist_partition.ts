// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { PersistSource } from "@malloydata/malloy";

/**
 * The `partition=` key on a `#@ persist` tag: the columns a storage build lays
 * the destination table out by.
 *
 * It is a LAYOUT choice and carries no isolation. Every dynamic term a source
 * strips is re-applied when the artifact is read, whether or not its column is
 * partitioned; a partition column only decides which of those terms prune files
 * before they are read and which filter rows after. So a partition list that
 * omits the column a caller is scoped by costs a full scan, never a leak — and
 * nothing here needs to prove otherwise.
 *
 * That is also why the list is free rather than derived. An earlier sketch made
 * the equality columns of the stripped terms the MINIMUM partition set, which
 * forces a partition per user for a source whose rows many users can see — not
 * a partition but a cross product.
 *
 * It lives on the `#@ persist` tag beside `storage=` because that tag already
 * decides what is built and where, and partitioning is a property of that
 * build.
 */
export type PartitionRefusal =
   | "partition_without_storage"
   | "partition_column_unknown"
   | "partition_column_not_public";

export type PartitionResolution =
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
 * The relation is the source's PUBLIC projection — `projectToPublicColumns`
 * narrows the build to it — so a partition column outside that surface is not
 * merely undesirable, it does not exist in the table the `ALTER TABLE … SET
 * PARTITIONED BY` would run against. Refusing here turns what would be an
 * opaque mid-build SQL error into a publish-time message naming the column.
 *
 * `partition=` without `storage=` is refused rather than ignored: a colocated
 * build writes into the customer's own warehouse, where partitioning is that
 * warehouse's DDL and outside this mechanism entirely, so honouring the key
 * silently is the one outcome that would mislead.
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
 * The source's column names, split into the ones it exposes publicly and the
 * full set it declares — the two the refusals above distinguish between.
 *
 * Reads the compiled `SourceDef.fields` rather than `_explore.intrinsicFields`
 * precisely because it needs the hidden ones: an access-restricted column is
 * present there with a non-public `accessModifier`, which is what separates
 * "you named a column the source hides" from "you named nothing at all". A
 * column removed with `except:` is absent from the list altogether and so
 * reports as unknown; the refusal's wording says so rather than implying the
 * author mistyped.
 *
 * Degrades to empty sets on unreadable IR, which refuses every declared column
 * as unknown. That is the fail-closed direction: `partition=` is not load
 * bearing for correctness, so refusing the build is a cost, while emitting DDL
 * against columns this pass could not see is a mid-build failure with no
 * message worth reading.
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
