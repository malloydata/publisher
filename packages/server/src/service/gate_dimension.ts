// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Shared given-expansion and structural-warning helpers for the SOURCE-LINE
 * form of `#(authorize)` — the given a struct's gate expression transitively
 * reaches through a field reference, and whether that expression negates a
 * membership test. Originally home to the DIMENSION form's own discovery and
 * load-time validation as well (a boolean dimension annotated in FIELD
 * position, referenced by NAME at graft time); that form and everything
 * specific to it were deleted in Task 6 — see `git log` for this file's
 * history if you need the discovery/validation shape it once carried.
 *
 * `resolveFieldUsagePath`'s join-following walk, and {@link
 * expandRefSummaryGivenIds} built on it, survive because the source-line
 * form's own G4/W1/W2 (`validateSourceLineGateGivenUsage` below) need the
 * identical non-transitive expansion: a gate that is a bare field reference
 * to another dimension (`#(authorize) authorized` over `dimension: authorized
 * is org_id in $GROUPS`) carries `fieldUsage` but no `givenUsage` at all, and
 * a naive G4/W1 that only read the raw `refSummary.givenUsage` would pass a
 * defaulted given hidden one hop away.
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
 * Result of {@link expandRefSummaryGivenIds}: either the full
 * transitively-reachable given id set, or a signal that some referenced field
 * path could not be resolved. `ok: false` must NEVER be read as "zero
 * givens" — an unresolvable reference means the expansion is incomplete, not
 * that the gate references nothing, and a caller that fell back to an empty
 * set here would silently bypass G4 and the request-time given-unbound
 * backstop for exactly the gate they exist to check.
 */
export type GivenIdExpansion =
   | { ok: true; givenIds: Set<string> }
   | { ok: false; unresolvedPath: string };

/**
 * Resolve a `refSummary.fieldUsage` path (source-rooted, per-segment name —
 * `["h", "ok"]` for `h.ok`) against `struct`, following a JOINED field's own
 * embedded struct for every segment but the last. This is given-expansion
 * ONLY — it does not widen gate discovery — it lets a gate's OWN expression
 * correctly account for a given reached through a join it references
 * (`authorized is h.ok`), which a leaf-only match could not see at all.
 *
 * Matches each segment by full name, not by scanning `struct.fields` for
 * something merely named like the leaf — the bug this replaces would let an
 * unrelated LOCAL field that happens to share the leaf's name (e.g. `struct`
 * has its own `ok`) silently substitute for the join-qualified `h.ok`.
 *
 * An intermediate segment may also be a record- or array-typed dimension
 * (`rec.a`, `tags.each`): Malloy marks those `join` too but they are not
 * `SourceDef`s, so requiring `isSourceDef` on every segment refused such a
 * gate at load even though its given resolves fine. They carry their own
 * `fields`, so the descent is the same one — and an unresolvable NON-EMPTY
 * path still returns `undefined` rather than resolving to nothing, which is
 * what G4 rests on.
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

/** The two `refSummary` slots {@link expandRefSummaryGivenIds} reads —
 *  matches the shape both a `FieldDef`'s own `refSummary` and a compiled
 *  `FilterCondition`'s own `refSummary` carry. Exported for `./authorize`'s
 *  callers, which read this shape off a duck-typed `CompiledGateCondition`
 *  rather than Malloy's own (unexported) `FilterCondition`. */
export type ExpandableRefSummary = {
   fieldUsage?: { path: string[] }[];
   givenUsage?: { id: string }[];
};

/**
 * Every given id transitively reachable from `refSummary` — a raw IR
 * reference summary — resolved against `struct`. Malloy's own
 * `refSummary.givenUsage` is NOT transitive — a bare-reference wrapper
 * dimension (`authorized is base_authorized`) carries no `givenUsage` of its
 * own even though `base_authorized` does, and the SAME gap applies to a
 * compiled `FilterCondition`'s own top-level `refSummary` — so this walks
 * `refSummary.fieldUsage` recursively (through {@link resolveFieldUsagePath},
 * so a join-qualified reference like `h.ok` is followed into `h`'s own struct
 * rather than silently dropped) and unions in each referenced field's own
 * `givenUsage`. `seen` is keyed by FIELD OBJECT IDENTITY, not name — a
 * name-keyed set would misreport a cycle the first time a joined struct
 * happens to reuse a local field name (e.g. both `struct` and a join both
 * declare `org_id`), truncating the expansion.
 *
 * Returns `{ok: false}` — never an empty set — when a referenced path cannot
 * be resolved at all; see {@link GivenIdExpansion}'s doc for why that
 * distinction is load-bearing.
 *
 * Exported for `./gate_classification`'s `resolveGateShape`, which runs this
 * against a LIFTED `FilterCondition`'s own `refSummary`.
 */
export function expandRefSummaryGivenIds(
   struct: SourceDef,
   refSummary: ExpandableRefSummary | undefined,
   seen: Set<FieldDef> = new Set(),
): GivenIdExpansion {
   const ids = new Set<string>();
   for (const g of refSummary?.givenUsage ?? []) ids.add(g.id);
   for (const usage of refSummary?.fieldUsage ?? []) {
      // Malloy emits a synthetic `fieldUsage` entry with an empty `path` for
      // a function call (e.g. `upper(region)`) — it carries no field
      // reference to resolve. The real field the call reads (`region`)
      // arrives as its own separate, non-empty-path entry, so skipping this
      // one loses nothing; treating it as unresolvable would wrongly refuse
      // every gate containing a function call.
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
 * Whether `node` (a compiled boolean expression, or a sub-tree of one)
 * contains a negated membership test (`not (x in $Y)`) — W2's shape, where an
 * EMPTY given then matches every row rather than none. A narrow structural
 * scan over exactly the node kinds a boolean predicate can compose from
 * (`and`/`or`/`not`/`()`/`inGiven`).
 *
 * Exported for the string-form-era doc references and re-used unchanged by
 * {@link validateSourceLineGateGivenUsage}'s W2.
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
      // Unwrap `()` before checking — `not (x in $Y)` compiles the
      // parenthesized operand as its own `()` node, so checking `n.e`
      // directly (without descending through it first) missed this shape.
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

/** Non-fatal {@link validateSourceLineGateGivenUsage} findings — ridden on
 *  the same metric channel `./authorize`'s `RowLevelGateRejectionCause`
 *  uses. */
export type SourceLineGateWarningCause =
   | "source_line_gate_no_given_reference"
   | "source_line_gate_negated_membership";

/**
 * G4/W1/W2 for the SOURCE-LINE form of `#(authorize)`: the load-time
 * given-default refusal and the two non-fatal warnings, run against a
 * source-line gate's lifted probe condition.
 *
 * G3 (every referenced given resolves) needs no separate check here — it is
 * covered by Malloy's own compile error on the very probe this reads
 * `refSummary`/`conditionExpr` off (`./authorize`'s `liftRowLevelCondition`):
 * an unreachable given fails that compile before this function is ever
 * reached. `declaredGivenNames`-reachability therefore has no counterpart
 * here either — the probe already compiled against the graft target's real
 * field scope.
 *
 * Reuses {@link expandRefSummaryGivenIds} rather than reading
 * `refSummary.givenUsage` directly, for the identical non-transitivity
 * reason `./gate_classification`'s request-time `resolveGateShape` does: a
 * gate that is a bare field reference to another dimension (`#(authorize)
 * authorized` over `dimension: authorized is org_id in $GROUPS`) carries
 * `fieldUsage` but no `givenUsage` at all, and a G4 that only read the raw
 * field would pass a defaulted given hidden one hop away. W1 is therefore
 * "the EXPANDED given set is empty", never raw `givenUsage.length === 0` —
 * those differ for exactly this shape (confirmed: `1 = 1`/`org_id = 999`
 * carry neither `fieldUsage` nor `givenUsage` and correctly expand to empty;
 * `#(authorize) authorized` over a given-referencing wrapper dimension
 * carries `fieldUsage` alone and must NOT be mistaken for W1).
 *
 * `struct` must be the gate's OWN declaring source's compiled struct — the
 * same one `buildRowLevelProbe` grafted the probe onto, so `refSummary`'s
 * field paths are already resolved against it. Throws `ModelCompilationError`
 * naming `sourceName` on G4 or an unresolvable reference — fail-closed: an
 * unresolvable reference is refused, never silently treated as referencing no
 * given. `onWarning` fires for W1/W2, which do not fail the load.
 */
export function validateSourceLineGateGivenUsage(
   sourceName: string,
   struct: SourceDef,
   refSummary: ExpandableRefSummary | undefined,
   conditionExpr: unknown,
   modelDef: ModelDef,
   onWarning?: (cause: SourceLineGateWarningCause, detail: string) => void,
): void {
   const expansion = expandRefSummaryGivenIds(struct, refSummary);
   if (!expansion.ok) {
      throw new ModelCompilationError({
         message:
            `#(authorize) on source "${sourceName}" references ` +
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
               `#(authorize) on source "${sourceName}" references ` +
               `\`$${given.name}\`, which is declared with a default. A ` +
               `caller who supplies no value for \`$${given.name}\` gets ` +
               `that default, which can admit rows the gate was meant to ` +
               `exclude. Declare \`$${given.name}\` with no default`,
         });
      }
   }

   if (givenIds.size === 0) {
      onWarning?.(
         "source_line_gate_no_given_reference",
         `#(authorize) on source "${sourceName}" references no given; it is ` +
            `a fixed predicate, not an access rule keyed on the caller`,
      );
   }
   if (containsNegatedMembership(conditionExpr)) {
      onWarning?.(
         "source_line_gate_negated_membership",
         `#(authorize) on source "${sourceName}" negates a membership test; ` +
            `an empty given then matches every row instead of none`,
      );
   }
}
