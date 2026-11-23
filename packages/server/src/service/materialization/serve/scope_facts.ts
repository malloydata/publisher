// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What read admission reads of a query's source that does not depend on the
 * query, recorded at each bind commit for every named source of the model,
 * so a query looks it up instead of walking the author's IR:
 *
 * - what the source's entry-point gates and row scope read
 *   (`gateReadsOfSource`), for each set of stored sources an answer can
 *   read: the served copy's lane (the sources it re-roots), each bound table
 *   alone, and none;
 * - whether the source carries an `#(authorize)` or `#(access_filter)` gate
 *   anywhere beneath it (the source's half of `queryReadsAccess`);
 * - whether its entry point reads a row filter declared on a composite.
 *
 * Kept per model runtime, for the snapshot it was recorded over: a query
 * pinned to another snapshot, on a source written inline, or reading a set
 * of stored sources nothing recorded walks as before.
 */

import type { ModelDef } from "@malloydata/malloy";
import { parseAuthorizeAnnotation } from "../../authorize";
import { rowFilterOnComposite } from "../../composite_row_filter";
import { resolveStructRefSource } from "../../governance/ir/prepared_query";
import type { Binding, Snapshot } from "../domain/binding";
import { gateReadsOfSource } from "./gate_reads";
import type { GateReads } from "./gate_reads_types";

/** One named source's flags. */
interface SourceFlags {
   /** The source carries a gate anywhere beneath it ({@link carriesAGate}). */
   readonly carriesGate: boolean;
   /** Its entry point reads a row filter declared on a composite. */
   readonly compositeRowFilter: boolean;
}

/** The served copy's lane 0: the stored sources it reads, and those it re-roots. */
export interface ScopeLane {
   readonly stored: ReadonlySet<string>;
   readonly rerooted: ReadonlySet<string>;
}

/** What a bind commit recorded for one model. */
export interface RecordedScope {
   readonly snapshot: Snapshot;
   /** The tables `snapshot` binds, for a snapshot rebuilt over the same ones. */
   readonly tables: string;
   readonly lane?: ScopeLane;
   readonly flags: ReadonlyMap<string, SourceFlags>;
   /** Gate reads by {@link readsKey}; a source whose reads no stored set changes under `name` alone. */
   readonly reads: ReadonlyMap<string, GateReads | undefined>;
}

const recorded = new WeakMap<object, RecordedScope>();

/** The key of one source's gate reads over a stored and a re-rooted set. */
function readsKey(
   name: string,
   stored?: ReadonlySet<string>,
   rerooted?: ReadonlySet<string>,
): string {
   if (stored === undefined) return name;
   return [
      name,
      [...stored].sort().join("\n"),
      [...(rerooted ?? [])].sort().join("\n"),
   ].join("\0");
}

/** Whether a source's gate reads name nothing any stored set could change. */
function readsNothing(r: GateReads | undefined): boolean {
   return (
      r !== undefined &&
      !r.unresolved &&
      !r.scoped &&
      !r.gated &&
      r.columns.length === 0 &&
      r.names.size === 0 &&
      r.entryPaths.length === 0
   );
}

/**
 * Record, for `runtime`'s model, every named source's scope facts over the
 * tables `snapshot` binds. Called by each bind commit; never throws.
 */
export function recordScopeFacts(
   runtime: object,
   args: { modelDef: ModelDef; snapshot: Snapshot; lane?: ScopeLane },
): void {
   const { modelDef, snapshot, lane } = args;
   const flags = new Map<string, SourceFlags>();
   const reads = new Map<string, GateReads | undefined>();
   const empty = new Set<string>();
   const storedSets: ReadonlySet<string>[] = [];
   const seen = new Set<string>();
   const addSet = (ids: readonly string[]) => {
      const set = new Set(ids);
      const key = readsKey("", set, empty);
      if (seen.has(key)) return;
      seen.add(key);
      storedSets.push(set);
   };
   for (const b of snapshot.bindings as readonly Binding[]) {
      addSet(b.sourceIDs ?? []);
   }
   for (const name of Object.keys(modelDef.contents ?? {})) {
      const struct = resolveStructRefSource<unknown>(name, modelDef);
      if (struct === undefined) continue;
      flags.set(name, {
         carriesGate: carriesAGate(struct),
         compositeRowFilter: compositeOf(struct, modelDef),
      });
      const none = gateReadsOfSource(struct, modelDef, empty, empty);
      if (readsNothing(none)) {
         reads.set(readsKey(name), none);
         continue;
      }
      reads.set(readsKey(name, empty, empty), none);
      for (const set of storedSets) {
         const key = readsKey(name, set, empty);
         if (!reads.has(key)) {
            reads.set(key, gateReadsOfSource(struct, modelDef, set, empty));
         }
      }
      if (lane) {
         reads.set(
            readsKey(name, lane.stored, lane.rerooted),
            gateReadsOfSource(struct, modelDef, lane.stored, lane.rerooted),
         );
      }
   }
   recorded.set(runtime, {
      snapshot,
      tables: tablesOf(snapshot),
      ...(lane ? { lane } : {}),
      flags,
      reads,
   });
}

/** What the last bind commit recorded for `runtime`'s model over `snapshot`. */
export function scopeFactsOf(
   runtime: object | undefined,
   snapshot: object | undefined,
): RecordedScope | undefined {
   if (runtime === undefined) return undefined;
   const r = recorded.get(runtime);
   if (r === undefined || snapshot === undefined) return undefined;
   if (r.snapshot === snapshot) return r;
   // A snapshot rebuilt over the same tables (a model whose bindings are
   // set beside its package's) holds the same facts.
   return tablesOf(snapshot as Snapshot) === r.tables ? r : undefined;
}

const tablesMemo = new WeakMap<object, string>();

/**
 * The tables a snapshot binds, as a key: the facts read only these (and the
 * model), never the manifest's entries.
 */
function tablesOf(snapshot: Snapshot): string {
   let key = tablesMemo.get(snapshot);
   if (key === undefined) {
      key = snapshot.bindings
         .map((b) => (b as Binding).virtualHandle)
         .sort()
         .join("\n");
      tablesMemo.set(snapshot, key);
   }
   return key;
}

/**
 * The recorded gate reads of source `name` over the stored and re-rooted
 * sets, or `unrecorded`.
 */
export function recordedReads(
   facts: RecordedScope,
   name: string,
   stored: ReadonlySet<string>,
   rerooted: ReadonlySet<string>,
): GateReads | undefined | "unrecorded" {
   const any = readsKey(name);
   if (facts.reads.has(any)) return facts.reads.get(any);
   const key = readsKey(name, stored, rerooted);
   return facts.reads.has(key) ? facts.reads.get(key) : "unrecorded";
}

/** Whether the entry point reads a row filter declared on a composite; fails closed. */
function compositeOf(struct: unknown, modelDef: unknown): boolean {
   try {
      return rowFilterOnComposite(struct, modelDef);
   } catch {
      return true;
   }
}

/**
 * A rendering of gate reads for comparing two of them: every field, sets
 * sorted. Undefined reads render as undefined.
 */
export function showReads(r: GateReads | undefined): string | undefined {
   if (r === undefined) return undefined;
   return JSON.stringify(r, (_k, v: unknown) =>
      v instanceof Set ? [...v].sort() : v instanceof Map ? [...v].sort() : v,
   );
}

/** Max IR depth {@link carriesAGate} walks; past it, a gate is assumed. */
const MAX_GATE_WALK_DEPTH = 200;

/**
 * Whether a slice of IR carries an `#(authorize)` or `#(access_filter)`
 * note anywhere (the plan's gate walk, `referencesAuthorize`, read with the
 * same parser). Fails closed.
 */
export function carriesAGate(
   node: unknown,
   seen: WeakSet<object> = new WeakSet(),
   depth = 0,
): boolean {
   if (depth > MAX_GATE_WALK_DEPTH) return true;
   if (node === null || typeof node !== "object") return false;
   if (seen.has(node)) return false;
   seen.add(node);
   if (Array.isArray(node)) {
      return node.some((item) => carriesAGate(item, seen, depth + 1));
   }
   const record = node as Record<string, unknown>;
   for (const key of ["blockNotes", "notes"]) {
      const notes = record[key];
      if (!Array.isArray(notes)) continue;
      for (const note of notes) {
         const text =
            typeof note === "string"
               ? note
               : (note as { text?: unknown } | null)?.text;
         if (typeof text !== "string") continue;
         try {
            if (parseAuthorizeAnnotation(text) !== null) return true;
         } catch {
            return true;
         }
      }
   }
   return Object.values(record).some((v) => carriesAGate(v, seen, depth + 1));
}
