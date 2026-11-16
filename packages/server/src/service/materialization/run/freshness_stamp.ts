// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * A standalone run's freshness facts: the declared `freshness.window` and
 * `freshness.fallback` of each table's sources, stamped on the entries of the
 * manifest it commits, with the instant the table's data is as of, so the
 * serve path ages the table exactly as it ages one a host's manifest
 * describes (domain/freshness.ts).
 *
 * Who declares: a standalone run is the only party that does, so it stamps
 * every entry from the declarations as they are now (a carried table keeps
 * its anchor and takes the current window). A host-orchestrated run stamps
 * nothing: its host distributes the manifest it binds, freshness included,
 * and that manifest is bound as the host wrote it.
 *
 * Pure: the caller passes the plan, the entries and the instant.
 */

import {
   declaredFreshnessFacts,
   freshnessOf,
   type DeclaredFreshness,
   type DeclaredFreshnessFacts,
} from "../domain/freshness";
import type { BuildPlan, ManifestEntry } from "../domain/types";

/** What stamping a run's entries reads from its build plan. */
interface DeclaredFreshnessIndex {
   /** The declared facts of the table at a content address, if any. */
   factsAt(sourceEntityId: string): DeclaredFreshnessFacts | undefined;
   /** The content addresses a table at `sourceEntityId` is built from. */
   upstreamsOf(sourceEntityId: string): readonly string[];
}

/**
 * The declared freshness of every planned table, by content address: the
 * plan sources sharing one table combine as {@link declaredFreshnessFacts}
 * says. A plan that cannot be read declares nothing.
 */
export function declaredFreshnessIndex(
   plan: Pick<BuildPlan, "sources" | "graphs"> | null | undefined,
): DeclaredFreshnessIndex {
   const declared = new Map<string, DeclaredFreshness[]>();
   const addressOf = new Map<string, string>();
   for (const [sourceID, source] of Object.entries(plan?.sources ?? {})) {
      addressOf.set(sourceID, source.sourceEntityId);
      const list = declared.get(source.sourceEntityId) ?? [];
      if (source.freshness) list.push(source.freshness as DeclaredFreshness);
      declared.set(source.sourceEntityId, list);
   }
   const upstreams = new Map<string, Set<string>>();
   for (const graph of plan?.graphs ?? []) {
      for (const level of graph.nodes ?? []) {
         for (const node of level) {
            const address = addressOf.get(node.sourceID);
            if (address === undefined) continue;
            const set = upstreams.get(address) ?? new Set<string>();
            for (const dep of node.dependsOn ?? []) {
               const upstream = addressOf.get(dep);
               if (upstream !== undefined && upstream !== address) {
                  set.add(upstream);
               }
            }
            upstreams.set(address, set);
         }
      }
   }
   const facts = new Map<string, DeclaredFreshnessFacts | undefined>();
   return {
      factsAt(sourceEntityId) {
         if (!facts.has(sourceEntityId)) {
            facts.set(
               sourceEntityId,
               declaredFreshnessFacts(declared.get(sourceEntityId) ?? []),
            );
         }
         return facts.get(sourceEntityId);
      },
      upstreamsOf(sourceEntityId) {
         return [...(upstreams.get(sourceEntityId) ?? [])];
      },
   };
}

/**
 * Whether a standalone run must rebuild a table it would otherwise carry
 * forward unchanged: its declared window has passed at `at`, or it declares a
 * window and its entry has no anchor to age it by (built before tables were
 * stamped). A table with no window is never past it.
 */
export function pastDeclaredWindow(
   prior: Pick<ManifestEntry, "dataAsOf">,
   facts: DeclaredFreshnessFacts | undefined,
   at: Date,
): boolean {
   if (facts?.freshnessWindowSeconds === undefined) return false;
   if (prior.dataAsOf === undefined) return true;
   return (
      freshnessOf(
         {
            dataAsOf: prior.dataAsOf,
            freshnessWindowSeconds: facts.freshnessWindowSeconds,
         },
         at,
      ) !== "fresh"
   );
}

/** An entry with no freshness facts. */
function withoutFreshness(entry: ManifestEntry): ManifestEntry {
   const {
      dataAsOf: _dataAsOf,
      freshnessWindowSeconds: _window,
      freshnessFallback: _fallback,
      ...rest
   } = entry;
   return rest;
}

/**
 * A standalone run's entries, each stamped from its table's declarations:
 * the declared window and fallback, and `dataAsOf`, the instant its data is
 * as of. A carried entry keeps its anchor. A built one is anchored at the
 * run's start, or at the oldest known anchor of a table it was built from,
 * directly or through others, when that is older (its rows are no newer than
 * what it read; a carried table that declares nothing has no known anchor
 * and adds none). An entry
 * whose table declares nothing carries no facts, so it is never stale.
 */
export function stampDeclaredFreshness(
   entries: Readonly<Record<string, ManifestEntry>>,
   index: DeclaredFreshnessIndex,
   carried: ReadonlySet<string>,
   runStartedAt: Date,
   /** Stored tables a build read that the plan's edges do not name. */
   readFrom?: ReadonlyMap<string, readonly string[]>,
): Record<string, ManifestEntry> {
   const stamped: Record<string, ManifestEntry> = {};
   const anchors = new Map<string, string | undefined>();
   const visiting = new Set<string>();
   // The anchor each entry is stamped with, upstreams first.
   const anchorOf = (address: string): string | undefined => {
      if (anchors.has(address)) return anchors.get(address);
      const entry = entries[address];
      if (!entry) return undefined;
      if (carried.has(address)) {
         anchors.set(address, entry.dataAsOf);
         return entry.dataAsOf;
      }
      if (visiting.has(address)) return runStartedAt.toISOString();
      visiting.add(address);
      let oldest = runStartedAt.getTime();
      for (const upstream of [
         ...index.upstreamsOf(address),
         ...(readFrom?.get(address) ?? []),
      ]) {
         const at = anchorOf(upstream);
         const ms = at === undefined ? NaN : Date.parse(at);
         if (!Number.isNaN(ms) && ms < oldest) oldest = ms;
      }
      visiting.delete(address);
      const anchor = new Date(oldest).toISOString();
      anchors.set(address, anchor);
      return anchor;
   };
   for (const [address, entry] of Object.entries(entries)) {
      const facts = index.factsAt(address);
      const bare = withoutFreshness(entry);
      if (facts === undefined) {
         stamped[address] = bare;
         continue;
      }
      const dataAsOf = anchorOf(address);
      stamped[address] = {
         ...bare,
         ...(dataAsOf !== undefined ? { dataAsOf } : {}),
         ...facts,
      };
   }
   return stamped;
}
