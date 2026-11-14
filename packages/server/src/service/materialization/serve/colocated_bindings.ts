// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Serve bindings for colocated `#(materialize)` tables (materialization v2,
 * phase 3 pull request 6): a table built into the source's own warehouse (no
 * `storage=`), read by the served copy on the source's own connection.
 *
 * A colocated table is recorded in the colocated build manifest, keyed by the
 * build source's content address, with no schema. A `#(materialize)` build
 * source is read by no author query, so Malloy's own manifest substitution
 * never serves it; the served copy does, re-rooting the author's source on
 * the table exactly as it re-roots a `storage=` source on its destination,
 * so every served-copy feature (stored computed dimensions, inline joins,
 * pair tables, hidden fields, read-time terms, gates proven on the served
 * compile) applies to it too.
 *
 * Each binding is keyed as a storage binding is, by the table's content
 * address (`virtualHandle`) on its connection (`destinationName`), with
 * `colocated` set: the served copy reaches it through the package's own
 * connection of that name, never a server-only alias, since an author can
 * already read their own warehouse.
 *
 * v1's colocated `#@ persist` is bound here (the v1 cutover, gap G1) when
 * the caller passes `v1Schema`: each planned v1 source without `storage=`
 * whose table the manifest names is bound with the schema its build captured
 * ({@link FreshnessAwareManifestEntry.schema}), or, for a table built before
 * the capture, the schema derived from its compiled output
 * ({@link v1StoredColumnsOf}).
 */

import { quoteManifestTablePath } from "../../quoting";
import type { MaterializePlan } from "../plan/build_file_generator";
import {
   type StoredColumn,
   storedColumnsOfOutput,
} from "../stored_column_types";
import type { FreshnessAwareManifestEntry, FreshnessManifest } from "../types";
import type { ServeBinding } from "./materialization_serve_transform";

/** What a binding reads of a build-plan source. */
interface ColocatedPlanSource {
   name: string;
   sourceEntityId: string;
   connectionName: string;
   dialect?: string;
   origin?: "persist" | "preaggregate";
   annotationFields?: Record<string, string>;
}

/**
 * The served copy's bindings for the colocated `#(materialize)` tables the
 * bound manifest holds: one per planned `#(materialize)` source without
 * `storage=` whose content address the manifest names, admitted by
 * `eligible` (the same positive eligibility a `storage=` binding needs).
 *
 * A plan with no compiled output columns ({@link MaterializePlan.storedColumns})
 * binds nothing: its table's columns are unknown, so it serves live.
 */
export function deriveColocatedServeBindings(args: {
   /** The bound colocated manifest, freshness fields included. */
   entries: FreshnessManifest | undefined;
   /** The build plan's sources, by sourceID. */
   planSources: Readonly<Record<string, ColocatedPlanSource>>;
   /** The `#(materialize)` plans, by the author's sourceID. */
   materializePlans: Readonly<Record<string, MaterializePlan>>;
   /** Whether the bind admits a planned source (positive eligibility). */
   eligible: (sourceID: string) => boolean;
   /**
    * The v1 cutover (G1): a v1 `#@ persist` source's stored columns derived
    * from its compiled output, for a table whose build captured no schema;
    * undefined when it cannot be derived. Absent, no v1 source is bound.
    */
   v1Schema?: (sourceID: string) => StoredColumn[] | undefined;
   /**
    * Planned sources that are not v1 `#@ persist` sources of the author's
    * (a persisted view's generated build source), never bound as v1.
    */
   notV1?: (sourceID: string) => boolean;
}): ServeBinding[] {
   const { entries, planSources, materializePlans } = args;
   if (!entries) return [];
   const bindings: ServeBinding[] = [];
   for (const [sourceID, source] of Object.entries(planSources)) {
      const plan = materializePlans[sourceID];
      if (source.origin === "preaggregate") continue;
      if (!plan && (!args.v1Schema || args.notV1?.(sourceID))) continue;
      // A `storage=` table is bound from the storage manifest, never here.
      if (
         plan?.persist.storage !== undefined ||
         source.annotationFields?.storage?.trim()
      ) {
         continue;
      }
      const entry = entries[source.sourceEntityId];
      if (!entry?.tableName) continue;
      const schema = plan
         ? plan.storedColumns
         : v1ColumnsOf(entry, () => args.v1Schema?.(sourceID));
      if (!schema?.length) continue;
      // Built on another connection than the plan names (a manifest from
      // another configuration): not this source's table.
      if (
         entry.connectionName !== undefined &&
         entry.connectionName !== source.connectionName
      ) {
         continue;
      }
      if (!args.eligible(sourceID)) continue;
      bindings.push({
         sourceName: source.name,
         sourceIDs: [sourceID],
         origin: "persist",
         destinationName: source.connectionName,
         colocated: true,
         virtualHandle: source.sourceEntityId,
         // Canonical for the connection's dialect, as Malloy's manifest
         // substitution pastes it (a reserved or case-preserved name quoted).
         tablePath: source.dialect
            ? quoteManifestTablePath(entry.tableName, source.dialect)
            : entry.tableName,
         schema: schema.map((c) => ({ ...c })),
         ...(entry.nativeSchema?.length
            ? { nativeSchema: entry.nativeSchema.map((c) => ({ ...c })) }
            : {}),
         ...(entry.dataAsOf !== undefined ? { freshAsOf: entry.dataAsOf } : {}),
         ...(entry.freshnessWindowSeconds !== undefined
            ? { freshnessWindowSeconds: entry.freshnessWindowSeconds }
            : {}),
         ...(entry.freshnessFallback !== undefined
            ? { freshnessFallback: entry.freshnessFallback }
            : {}),
      });
   }
   return bindings;
}

/**
 * A v1 colocated table's columns: what its build captured, else what its
 * compiled output derives (a table built before the capture).
 */
function v1ColumnsOf(
   entry: FreshnessAwareManifestEntry,
   derive: () => StoredColumn[] | undefined,
): StoredColumn[] | undefined {
   return entry.schema?.length ? entry.schema : derive();
}

/**
 * The columns a v1 colocated `#@ persist` build wrote, derived from the
 * source's compiled definition (the v1 cutover's G1, for a table built before
 * its build captured a schema). A v1 build source is query-shaped, so its
 * stored columns are the query's output fields. They are read from the
 * persisted query's output struct when the compiled source records it (what
 * the `extend` block drops with `except:` or `accept:` is still a column);
 * otherwise from the source's fields with no expression, minus views, with
 * two corrections that make the compiled field list read as the table:
 *
 * - a stored column is the field's name before any `rename:` (`as`), since
 *   the rename is applied in the source's `extend` block, after the build;
 * - a nest is a column (an array or record), though Malloy gives it a
 *   `join`, while every other joined field is not stored.
 *
 * A computed dimension in the `extend` block has an expression, so it is not
 * a column (the served copy keeps its definition). A field whose type has no
 * faithful stored spelling is left out ({@link storedColumnsOfOutput}), so
 * the served copy finds it missing and that source serves live.
 */
export function v1StoredColumnsOf(def: unknown): StoredColumn[] | undefined {
   const d = def as
      | {
           fields?: unknown[];
           dialect?: unknown;
           query?: { pipeline?: { outputStruct?: { fields?: unknown[] } }[] };
        }
      | undefined;
   // The persisted query's own output, when the compiled source records it:
   // exactly the build's columns, before the `extend` block renames, drops
   // (`except:`, `accept:`) or re-hides any (review F3).
   const output = d?.query?.pipeline?.at(-1)?.outputStruct?.fields;
   const fields = Array.isArray(output) ? output : d?.fields;
   if (!Array.isArray(fields)) return undefined;
   const outputs = fields
      .filter((f) => {
         const x = f as Record<string, unknown>;
         return (
            x !== null &&
            typeof x === "object" &&
            !("e" in x) &&
            x.type !== "turtle" &&
            (!("join" in x) || x.type === "array" || x.type === "record")
         );
      })
      .map((f) => {
         const { as: _as, ...rest } = f as Record<string, unknown>;
         return rest;
      });
   return storedColumnsOfOutput(outputs, {
      nested: d?.dialect === "duckdb",
      bigint: true,
      timestamptz: true,
   });
}
