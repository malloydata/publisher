// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Given-expansion and structural-warning helpers for the source-line form of
 * `#(authorize)`: which givens a gate expression transitively reaches through
 * field references, and whether it negates a membership test.
 */

import {
   isJoined,
   isSourceDef,
   type FieldDef,
   type ModelDef,
   type SourceDef,
} from "@malloydata/malloy";
import { ModelCompilationError } from "../errors";

/**
 * Result of {@link expandRefSummaryGivenIds}. `ok: false` must never be read
 * as "zero givens": the expansion is incomplete, and treating it as empty
 * would silently bypass G4 and the request-time given-unbound backstop.
 */
export type GivenIdExpansion =
   | { ok: true; givenIds: Set<string> }
   | { ok: false; unresolvedPath: string };

/**
 * Resolve a `refSummary.fieldUsage` path (`["h", "ok"]` for `h.ok`) against
 * `struct`, descending into each joined segment's struct. Used for
 * given-expansion only, not gate discovery.
 *
 * Every segment is matched by name along the path; matching the leaf alone
 * would let an unrelated local field of the same name stand in for `h.ok`.
 * Intermediate segments may be record/array dimensions (Malloy marks them
 * `join` but they are not `SourceDef`s); they carry `fields`, so the descent
 * is the same. An unresolvable path returns `undefined`, which G4 relies on.
 */
function resolveFieldUsagePath(
   struct: SourceDef,
   path: readonly string[],
): { struct: SourceDef; field: FieldDef } | undefined {
   let current = struct;
   for (let i = 0; i < path.length - 1; i++) {
      const seg = path[i];
      const joinField = (current.fields ?? []).find(
         (f) =>
            ((f as { as?: string }).as || f.name) === seg &&
            isJoined(f) &&
            (isSourceDef(f) || f.type === "record" || f.type === "array"),
      );
      if (!joinField) return undefined;
      current = joinField as unknown as SourceDef;
   }
   const leaf = path[path.length - 1];
   const field = (current.fields ?? []).find(
      (f) => ((f as { as?: string }).as || f.name) === leaf,
   );
   return field ? { struct: current, field } : undefined;
}

/** The `refSummary` slots {@link expandRefSummaryGivenIds} reads, shared by
 *  a `FieldDef` and a compiled `FilterCondition` (which Malloy does not
 *  export). */
export type ExpandableRefSummary = {
   fieldUsage?: { path: string[] }[];
   givenUsage?: { id: string }[];
};

/**
 * Every given id transitively reachable from `refSummary`, resolved against
 * `struct`. Malloy's `givenUsage` is not transitive (a wrapper dimension
 * `authorized is base_authorized` carries none of `base_authorized`'s), so
 * this recurses through `fieldUsage`, following joins. `seen` is keyed by
 * field object identity: a name-keyed set would report a false cycle when a
 * join reuses a local field name, truncating the expansion.
 *
 * Returns `{ok: false}`, never an empty set, when a path cannot be resolved
 * (see {@link GivenIdExpansion}).
 */
export function expandRefSummaryGivenIds(
   struct: SourceDef,
   refSummary: ExpandableRefSummary | undefined,
   seen: Set<FieldDef> = new Set(),
): GivenIdExpansion {
   const ids = new Set<string>();
   for (const g of refSummary?.givenUsage ?? []) ids.add(g.id);
   for (const usage of refSummary?.fieldUsage ?? []) {
      // Malloy emits an empty-path entry for a function call (`upper(region)`);
      // the field it reads arrives as its own entry. Treating this one as
      // unresolvable would refuse every gate containing a function call.
      if (usage.path.length === 0) continue;
      const resolved = resolveFieldUsagePath(struct, usage.path);
      if (!resolved) {
         return { ok: false, unresolvedPath: usage.path.join(".") };
      }
      if (seen.has(resolved.field)) continue;
      seen.add(resolved.field);
      const nestedRefSummary = (resolved.field as { refSummary?: unknown })
         .refSummary as ExpandableRefSummary | undefined;
      const nested = expandRefSummaryGivenIds(
         resolved.struct,
         nestedRefSummary,
         seen,
      );
      if (!nested.ok) return nested;
      for (const id of nested.givenIds) ids.add(id);
   }
   return { ok: true, givenIds: ids };
}

/**
 * Whether a compiled boolean expression contains a negated membership test
 * (`not (x in $Y)`), W2's shape: an empty given then matches every row
 * rather than none. Scans only `and`/`or`/`not`/`()`/`inGiven` nodes.
 */
export function containsNegatedMembership(node: unknown, depth = 0): boolean {
   if (depth > 64 || node === null || typeof node !== "object") return false;
   const n = node as {
      node?: unknown;
      not?: unknown;
      e?: unknown;
      kids?: unknown;
   };
   if (n.node === "inGiven" && n.not === true) return true;
   if (n.node === "not") {
      // `not (x in $Y)` compiles the operand as a `()` node; unwrap it.
      let inner = n.e as { node?: unknown; e?: unknown } | null | undefined;
      while (inner && inner.node === "()") {
         inner = inner.e as { node?: unknown; e?: unknown } | null | undefined;
      }
      if (inner && inner.node === "inGiven") return true;
      return containsNegatedMembership(n.e, depth + 1);
   }
   if (n.node === "and" || n.node === "or") {
      const kids = n.kids as { left?: unknown; right?: unknown } | undefined;
      return (
         containsNegatedMembership(kids?.left, depth + 1) ||
         containsNegatedMembership(kids?.right, depth + 1)
      );
   }
   if (n.node === "()") return containsNegatedMembership(n.e, depth + 1);
   return false;
}

/** Non-fatal {@link validateSourceLineGateGivenUsage} findings, reported on
 *  the same metric channel as `RowLevelGateRejectionCause`. */
export type SourceLineGateWarningCause =
   | "source_line_gate_no_given_reference"
   | "source_line_gate_negated_membership";

/**
 * Load-time checks for a source-line gate (either route), run against its
 * lifted probe condition: G4 refuses a referenced given that has a default;
 * W1 (no given referenced) and W2 (negated membership) are warnings.
 *
 * G3 (every given resolves) is already enforced by the probe's own compile.
 * The given set is the transitive expansion, not raw `givenUsage`: a gate
 * `#(authorize) authorized` over `dimension: authorized is org_id in $GROUPS`
 * carries only `fieldUsage`, so reading `givenUsage` would let a defaulted
 * given through G4 and misfire W1.
 *
 * `struct` must be the gate's declaring source's compiled struct (the one the
 * probe was grafted onto). Fails closed: an unresolvable reference throws
 * `ModelCompilationError`.
 */
export function validateSourceLineGateGivenUsage(
   sourceName: string,
   /** The gate's route, so messages name the annotation the author wrote. */
   route: string,
   struct: SourceDef,
   refSummary: ExpandableRefSummary | undefined,
   conditionExpr: unknown,
   modelDef: ModelDef,
   onWarning?: (cause: SourceLineGateWarningCause, detail: string) => void,
): void {
   const gate = `#(${route}) gate`;
   const expansion = expandRefSummaryGivenIds(struct, refSummary);
   if (!expansion.ok) {
      throw new ModelCompilationError({
         message:
            `The ${gate} on source "${sourceName}" references ` +
            `"${expansion.unresolvedPath}", which could not be resolved to a ` +
            `field this model can reach. An unresolvable reference is refused, ` +
            `not treated as referencing no given`,
      });
   }
   const givenIds = expansion.givenIds;
   for (const id of givenIds) {
      const given = modelDef.givens?.[id];
      if (
         given &&
         (given.default !== undefined || given.defaultText !== undefined)
      ) {
         throw new ModelCompilationError({
            message:
               `The ${gate} on source "${sourceName}" references ` +
               `\`$${given.name}\`, which is declared with a default. A ` +
               `caller who supplies no value for \`$${given.name}\` gets ` +
               `that default, which can admit what the gate was meant to ` +
               `exclude. Declare \`$${given.name}\` with no default`,
         });
      }
   }

   if (givenIds.size === 0) {
      onWarning?.(
         "source_line_gate_no_given_reference",
         `The ${gate} on source "${sourceName}" references no given; it is ` +
            `a fixed predicate, not an access rule keyed on the caller`,
      );
   }
   if (containsNegatedMembership(conditionExpr)) {
      onWarning?.(
         "source_line_gate_negated_membership",
         `The ${gate} on source "${sourceName}" negates a membership test; ` +
            `an empty given then matches everything instead of nothing`,
      );
   }
}
