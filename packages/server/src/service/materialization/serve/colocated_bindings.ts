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
 * already read their own warehouse. v1's colocated `#@ persist` is not bound
 * here; it is still served through Malloy's build manifest.
 */

import { quoteManifestTablePath } from "../../quoting";
import type { MaterializePlan } from "../plan/build_file_generator";
import type { FreshnessManifest } from "../types";
import type { ServeBinding } from "./materialization_serve_transform";

/** What a binding reads of a build-plan source. */
export interface ColocatedPlanSource {
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
}): ServeBinding[] {
   const { entries, planSources, materializePlans } = args;
   if (!entries) return [];
   const bindings: ServeBinding[] = [];
   for (const [sourceID, source] of Object.entries(planSources)) {
      const plan = materializePlans[sourceID];
      if (!plan || source.origin === "preaggregate") continue;
      // A `storage=` table is bound from the storage manifest, never here.
      if (
         plan.persist.storage !== undefined ||
         source.annotationFields?.storage?.trim()
      ) {
         continue;
      }
      const entry = entries[source.sourceEntityId];
      if (!entry?.tableName || !plan.storedColumns?.length) continue;
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
         schema: plan.storedColumns.map((c) => ({ ...c })),
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
