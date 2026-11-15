// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A stored table as serving reads it, under one source name, and how a run's
 * manifest entries become those bindings: one per name that reads the table,
 * each with the plan's sourceIDs for it, so a model binds only its own
 * source of that name.
 *
 * Made by the facade from a run's or a host's manifest, and by the served
 * copy for a chained build's text; read by every serving leg, `Model` and the
 * `storage=` build, which names a chain's stored upstreams by them. An entry
 * with no column list binds nothing.
 *
 * Pure.
 */

import type { ManifestEntry } from "./types";
import { storageTablePath } from "./storage_table_path";

/**
 * One stored table as serving reads it, under one source name, either derived
 * from a build's manifest entry ({@link deriveServeBindings}) or supplied by a host.
 */
export interface ServeBinding {
   /** The Malloy source name to rebind (`source: <sourceName> is ...`). */
   sourceName: string;
   /**
    * The sourceIDs of this package's own sources that the table materializes:
    * the build-plan sources with this name at this content address, from this
    * server's own compile (see {@link planSourceIds}). A model keeps the binding
    * only when its own source of that name carries one of them, so an unrelated
    * same-named source in another model is never served from this table, while
    * an imported source (which keeps its declaring model's sourceID) still is.
    * Absent, as on a hand-made binding, the name alone decides, as before.
    */
   sourceIDs?: string[];
   /**
    * What the run that wrote or carried this stored table found of its
    * source's declared primary key, by sourceID: true when no value repeats
    * and none is NULL. The join-to-rollup leg in a destination reads
    * a join_many keyed on that key only when true. Internal: absent on every
    * binding a host's manifest makes, and dropped when a run is about to
    * rewrite the table.
    */
   primaryKeys?: Record<string, boolean>;
   /**
    * `persist` for a modeler-annotated source, `preaggregate` for a synthesized
    * rollup; absent means `persist`. A rollup must not take the ordinary
    * author-model lookups (see {@link rollupServeBindings}).
    */
   origin?: "persist" | "preaggregate";
   /**
    * The storage destination the physical table lives in; for a
    * {@link colocated} table, the source's own connection.
    */
   destinationName: string;
   /**
    * A colocated `#(materialize)` table (colocated_bindings.ts): built into
    * the source's own warehouse, on the connection {@link destinationName}
    * names, which the served copy reads it through (never a server-only
    * alias); its {@link tablePath} is already canonical for that
    * connection's dialect, and its {@link schema} is the build's compiled
    * output. Only the served copy reads one. Absent on a `storage=` binding.
    */
   colocated?: true;
   /** The virtualMap handle for this source (its build-posture identity). */
   virtualHandle: string;
   /** Logical (unquoted) physical table path; quoted for DuckDB at bind time. */
   tablePath: string;
   /** Authoritative DuckDB columns captured post-build (raw DuckDB types). */
   schema: { name: string; type: string }[];
   /**
    * A colocated table's columns with their warehouse types as its build
    * captured them (`ManifestEntry.nativeSchema`): a big-integer column is
    * cast to exactly its type, and one with none is answered live (known
    * limitation 12). Absent for a `storage=` table, whose `schema` is native.
    */
   nativeSchema?: { name: string; type: string }[];
   /**
    * Columns read through a cast to their declared type, for a table an
    * older build typed. No binding the server makes carries one; the served
    * copy leaves a source whose binding does to live.
    */
   readCasts?: { column: string; castTo: string; native?: true }[];
   /** Optional freshness anchor (data-as-of instant); carried through verbatim. */
   freshAsOf?: string;
   /**
    * Gates this binding per query the same way the colocated serve does: a
    * stale binding with fallback `live`/`fail` is dropped from the shape.
    */
   freshnessWindowSeconds?: number;
   freshnessFallback?: "live" | "stale_ok" | "fail";
}

/**
 * Whether a model may serve `binding`: false only when the model's own source of
 * that name carries a sourceID the binding does not name, which is an unrelated
 * source that happens to share the name. A binding with no `sourceIDs`, a
 * rollup (its name is generated and names nothing in the author's model), and a
 * name the model does not define are left to the checks downstream, as before.
 */
export function bindingServesModelSource(
   binding: ServeBinding,
   contents: Record<string, { sourceID?: unknown } | undefined> | undefined,
): boolean {
   if (binding.origin === "preaggregate" || !binding.sourceIDs) return true;
   const sourceID = contents?.[binding.sourceName]?.sourceID;
   if (typeof sourceID !== "string" || sourceID.length === 0) return true;
   return binding.sourceIDs.includes(sourceID);
}

/**
 * The primary key checks a local run recorded on a stored entry (its own
 * source's, by sourceID): internal, set only by the committing run or a
 * restore from the store, never by a host's manifest (`splitManifestEntries`).
 */
function primaryKeysOf(
   entry: ManifestEntry,
): Record<string, boolean> | undefined {
   const keys = (entry as { keyUniqueness?: { parentKeys?: unknown } })
      .keyUniqueness?.parentKeys;
   if (!keys || typeof keys !== "object") return undefined;
   const out = Object.fromEntries(
      Object.entries(keys).filter(([, v]) => typeof v === "boolean"),
   ) as Record<string, boolean>;
   return Object.keys(out).length > 0 ? out : undefined;
}

export function deriveServeBindings(
   entries: Record<string, ManifestEntry>,
   aliasesBySourceName: Record<string, string[]>,
   /** A destination's schema by name (see storageTablePath); absent, or answering undefined, is `main`. */
   destinationSchema?: (destinationName: string) => string | undefined,
   /** The package's own sourceIDs for a name at a content address ({@link planSourceIds}). */
   sourceIDsFor?: (sourceName: string, sourceEntityId: string) => string[],
): ServeBinding[] {
   const bindings: ServeBinding[] = [];
   // Names that OWN a table. An alias never claims one, or the name would be
   // bound twice and declared twice in one serve shape.
   const builders = new Set(
      Object.values(entries)
         .map((entry) => entry.sourceName)
         .filter((name): name is string => !!name),
   );
   for (const entry of Object.values(entries)) {
      if (
         !entry.sourceName ||
         !entry.storageDestinationName ||
         !entry.physicalTableName
      ) {
         continue;
      }
      // The wire Column has optional name/type; keep only complete columns.
      const schema = (entry.schema ?? [])
         .filter((c) => c.name && c.type)
         .map((c) => ({ name: c.name as string, type: c.type as string }));
      if (schema.length === 0) continue;
      // Builder first so it wins any ordering downstream; deduplicated because
      // the builder is normally in its own alias group.
      const names = [
         entry.sourceName,
         ...(aliasesBySourceName[entry.sourceName] ?? []).filter(
            (name) => !builders.has(name),
         ),
      ].filter((name, i, all) => all.indexOf(name) === i);
      for (const sourceName of names) {
         // Set whenever there is a plan to read, even empty: an empty list
         // names no source, so the binding serves nothing (see planSourceIds).
         const sourceIDs = sourceIDsFor?.(sourceName, entry.sourceEntityId);
         bindings.push({
            sourceName,
            ...(sourceIDs !== undefined ? { sourceIDs } : {}),
            // Carried, not inferred: a manifest travels without its build plan.
            origin:
               entry.origin === "preaggregate" ? "preaggregate" : "persist",
            destinationName: entry.storageDestinationName,
            virtualHandle: entry.sourceEntityId,
            // Qualified with the attach alias: unqualified, it would resolve
            // against the serve session's default catalog.
            tablePath: storageTablePath(
               entry.storageDestinationName,
               entry.physicalTableName,
               destinationSchema?.(entry.storageDestinationName),
            ),
            schema,
            freshAsOf: entry.dataAsOf,
            freshnessWindowSeconds: entry.freshnessWindowSeconds,
            freshnessFallback: entry.freshnessFallback,
            ...(primaryKeysOf(entry)
               ? { primaryKeys: primaryKeysOf(entry) }
               : {}),
         });
      }
   }
   return bindings;
}
