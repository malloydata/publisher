// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The stored relations an access decision reads: every source a gated or
 * caller-scoped definition of the package depends on, at any depth.
 *
 * A table stored as of its build (its relation reads the current time) holds
 * the rows its build saw. When such a table feeds an access decision (a gate,
 * or a value a caller's given selects), a frozen clock is a grant that
 * outlives its time, so the eligibility gate refuses it (`current_time`).
 * This module finds which relations feed one, fail closed, across every model
 * of the package and what each imports.
 *
 * An ACCESS DEFINITION is a source or query of any model's contents that
 * carries an `#(authorize)` or `#(access_filter)` gate anywhere beneath it
 * (its own, inherited, joined), or reads a given anywhere (a `where:`, a
 * dimension, a measure, a join's `on`, a view). Its DEPENDENCIES are every
 * source its compiled definition names: the base it extends (`extends`), the
 * source a `->` query reads (`structRef.sourceID`), each join, each composite
 * member, each `#(materialize)` or rollup base, each persisted view's or
 * query's source, and transitively, each of theirs.
 */

import { readsAGiven } from "../../probe_compiler";
import { EVERY_SOURCE } from "../domain/compiled_build_plan";
import { irCarriesAGate } from "./materialization_eligibility";

/** Max IR depth to walk for names; deep enough for real models. */
const MAX_WALK_DEPTH = 200;

/** Keys whose string value names another source a definition depends on. */
const DEPENDENCY_KEYS: ReadonlySet<string> = new Set(["sourceID", "extends"]);

/** Keys that carry no dependency: locations and annotations. */
const SKIPPED_KEYS: ReadonlySet<string> = new Set([
   "at",
   "location",
   "annotations",
   "blockNotes",
   "notes",
]);

/**
 * Add to `out` the sourceID of every source an access definition of
 * `contents` (a compiled model's `_modelDef.contents`) depends on,
 * transitively, through the other definitions of `contents`. A model's
 * contents hold what it imports, so imported sources are walked as their own
 * definitions. Fails closed: a definition whose walk cannot finish counts as
 * an access definition, and every name it reaches is added.
 */
export function collectAccessReads(contents: unknown, out: Set<string>): void {
   if (contents === null || typeof contents !== "object") return;
   const defs = Object.values(contents as Record<string, unknown>).filter(
      (d): d is Record<string, unknown> => d !== null && typeof d === "object",
   );
   // Each definition's direct dependencies, by its own sourceID.
   const direct = new Map<string, Set<string>>();
   const access: Set<string>[] = [];
   for (const def of defs) {
      const own = typeof def.sourceID === "string" ? def.sourceID : undefined;
      const deps = new Set<string>();
      namesIn(def, deps, own, new WeakSet(), 0);
      if (own !== undefined) direct.set(own, deps);
      if (isAccessDefinition(def)) access.push(deps);
   }
   for (const deps of access) {
      const queue = [...deps];
      while (queue.length > 0) {
         const id = queue.pop()!;
         if (out.has(id)) continue;
         out.add(id);
         for (const next of direct.get(id) ?? []) {
            if (!out.has(next)) queue.push(next);
         }
      }
   }
}

/** Whether a definition carries a gate or reads a given, anywhere. Fails closed. */
function isAccessDefinition(def: unknown): boolean {
   if (irCarriesAGate(def)) return true;
   try {
      return readsAGiven(def);
   } catch {
      return true;
   }
}

/** Every source name a slice of IR names, but `self`'s own top-level name. */
function namesIn(
   node: unknown,
   out: Set<string>,
   self: string | undefined,
   seen: WeakSet<object>,
   depth: number,
): void {
   if (node === null || typeof node !== "object") return;
   if (depth > MAX_WALK_DEPTH) {
      out.add(EVERY_SOURCE);
      return;
   }
   if (seen.has(node)) return;
   seen.add(node);
   if (Array.isArray(node)) {
      for (const item of node) namesIn(item, out, self, seen, depth + 1);
      return;
   }
   for (const [key, value] of Object.entries(node)) {
      if (SKIPPED_KEYS.has(key)) continue;
      if (DEPENDENCY_KEYS.has(key) && typeof value === "string") {
         if (!(depth === 0 && key === "sourceID" && value === self)) {
            out.add(value);
         }
         continue;
      }
      namesIn(value, out, self, seen, depth + 1);
   }
}
