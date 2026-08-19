/**
 * Discovery and load-time validation for the DIMENSION form of `#(authorize)`
 * — a boolean dimension annotated in FIELD position (rather than a string
 * expression annotated on the `source:` line), referenced by NAME at graft
 * time instead of re-parsed. Kept deliberately separate from `./authorize`'s
 * string-form walker (`classifyAuthorizeGate`): a grafted dimension gate's
 * lifted filter condition is an UNRESOLVED field-reference node (`{node:
 * "field", path: ["authorized"]}`), not the dimension's own predicate tree —
 * see the spike doc cited by the task brief this module implements. Routing
 * this form through `classifyAuthorizeGate` would find no comparison/`inGiven`
 * node to classify and fail closed on every gate, which is why validation here
 * reads the dimension's own compiled `FieldDef` instead.
 *
 * `./gate_classification`'s `gateExprsForOwnAnnotations` is the request-time
 * counterpart: once a struct's gate dimension is found here (or, for an
 * inherited-unchanged one, simply because Malloy's `extend` already flattens
 * unchanged fields into the deriving struct's own `fields`), it grafts by
 * NAME (`quoteMalloyIdentifier`), never by the dimension's `code` — see
 * Constraint 9 in the task brief for why re-parsing `code` would reintroduce
 * the string-form's fragility.
 */

import {
   expressionIsScalar,
   isJoined,
   isSourceDef,
   type FieldDef,
   type ModelDef,
   type SourceDef,
} from "@malloydata/malloy";
import { ModelCompilationError } from "../errors";
import { ownLevelNotes } from "./annotations";
import { containsAuthorizeAnnotationTag } from "./authorize";
import { resolveDeclaredSource } from "./gate_registry_walk";

/** `field.as || field.name` — the name a graft or an error message uses.
 *  Exported for `./gate_classification`'s request-time graft, which needs
 *  the identical name lookup rather than a second copy of it. */
export function gateFieldName(field: FieldDef): string {
   return (field as { as?: string }).as || field.name;
}

/**
 * Every non-join field on `struct`'s OWN `fields` carrying a field-position
 * `#(authorize)` note. Never a join (Constraint 3 — joins never carry a
 * gate; a joined source's own gate dimension sits inside the join field's
 * embedded `StructDef` and this deliberately does not recurse into it).
 *
 * "Join" here means `isJoined(field) && isSourceDef(field)`, not bare
 * `isJoined` — Malloy's IR also sets the `join` marker on an array/record
 * -typed dimension's nested struct (confirmed by
 * `row_level_authorize.integration.spec.ts`'s array/record-dimension tests),
 * and neither is a real join with a joined SOURCE to exempt; both must still
 * be reachable here so `validateGateDimension`'s G1 refuses them for what
 * they are — a non-boolean dimension, not a legal gate.
 *
 * "Own `fields`" already covers the inheritance cases for free: Malloy
 * flattens an `extend`'s unchanged fields into the deriving struct's own
 * `fields` array (unlike annotations, which use a separate `inherits`
 * chain), so an inherited-unchanged gate dimension is found here exactly the
 * same way a freshly declared one is — no separate ancestor walk needed.
 */
export function findGateDimensionCandidates(struct: SourceDef): FieldDef[] {
   // `?? []`: a real compiled `SourceDef` always carries `fields`, but a
   // test double built to exercise unrelated logic (a run-target struct
   // stubbed as `{structRef: "name"}`) may not — this must read as "no
   // candidates", not throw and have the caller's `try`/`catch` in
   // `./gate_classification`'s `gateExprsForOwnAnnotations` mistake that for
   // a fail-closed gate.
   return (struct.fields ?? []).filter(
      (field) =>
         !(isJoined(field) && isSourceDef(field)) &&
         ownLevelNotes(field.annotations).some((note) =>
            containsAuthorizeAnnotationTag([note.text]),
         ),
   );
}

/**
 * Every given id transitively reachable from `field`'s expression on
 * `struct`. Malloy's own `refSummary.givenUsage` is NOT transitive — a
 * bare-reference wrapper dimension (`authorized is base_authorized`) carries
 * no `givenUsage` of its own even though `base_authorized` does (confirmed by
 * the spike this module implements, F5) — so this walks `refSummary
 * .fieldUsage` recursively and unions in each referenced field's own
 * `givenUsage`. `seen` (by name) guards a reference cycle.
 *
 * Exported for `./gate_classification`'s `gateExprsForOwnAnnotations`, which
 * needs the same expansion to populate `GateEntry.dimensionForm.givenNames`
 * (resolving each id to its `modelDef.givens[id].name` there, since this
 * function only knows ids).
 */
export function expandGivenIds(
   struct: SourceDef,
   field: FieldDef,
   seen: Set<string> = new Set(),
): Set<string> {
   const name = gateFieldName(field);
   const ids = new Set<string>();
   if (seen.has(name)) return ids;
   seen.add(name);
   const refSummary = (field as { refSummary?: unknown }).refSummary as
      | { fieldUsage?: { path: string[] }[]; givenUsage?: { id: string }[] }
      | undefined;
   for (const g of refSummary?.givenUsage ?? []) ids.add(g.id);
   for (const usage of refSummary?.fieldUsage ?? []) {
      const refName = usage.path[usage.path.length - 1];
      const refField = struct.fields.find((f) => gateFieldName(f) === refName);
      if (refField && refField !== field) {
         for (const id of expandGivenIds(struct, refField, seen)) {
            ids.add(id);
         }
      }
   }
   return ids;
}

/**
 * Whether `node` (a dimension's compiled expression, or a sub-tree of one)
 * contains a negated membership test (`not (x in $Y)`) — W2's shape, where an
 * EMPTY given then matches every row rather than none. A narrow structural
 * scan over exactly the node kinds a boolean dimension can compose from
 * (`and`/`or`/`not`/`()`/`inGiven`), not `classifyAuthorizeGate`'s full walk
 * — see this module's header for why the two must not share one walker.
 */
function containsNegatedMembership(node: unknown, depth = 0): boolean {
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

/** Non-fatal `validateGateDimension` findings — W1/W2, ridden on the same
 *  metric channel `./authorize`'s `RowLevelGateRejectionCause` uses. */
export type GateDimensionWarningCause =
   | "gate_dimension_no_given_reference"
   | "gate_dimension_negated_membership";

export interface GateDimensionResolution {
   name: string;
   field: FieldDef;
}

/**
 * Validate and resolve the gate dimension `struct` carries as an ENTRY
 * POINT — own, or inherited unchanged from an `extend` base (see
 * {@link findGateDimensionCandidates}'s doc). Returns `undefined` when
 * `struct` carries none at all. Throws `ModelCompilationError` — naming
 * `sourceName` — on any hard refusal (G1, private, more than one candidate,
 * a redefinition that silently sheds an inherited gate, G3, G4). `onWarning`
 * fires for W1/W2, which do not fail the load.
 *
 * `declaredGivenNames` is this model's OWN given surface — the SAME one
 * `classifyAuthorizeGate`'s `unreachable_given` check reads for the string
 * form (`ApiGiven[]`/`compiled.givens`, curated to one import hop), NOT
 * `modelDef.givens`: that raw IR registry contains every given the compiled
 * expression tree references AT ANY DEPTH (Malloy needs it there to
 * type-check), so it is NOT the "unreachable" signal — confirmed empirically
 * against a 2-import-hop fixture, where `modelDef.givens` still carried the
 * far given even though it is absent from `compiled.givens`.
 *
 * G3 is checked before G4 — an unreachable given's declared default is not
 * inspectable (Constraint: "G3 before G4" in the task brief).
 */
export function validateGateDimension(
   sourceName: string,
   struct: SourceDef,
   modelDef: ModelDef,
   declaredGivenNames: ReadonlySet<string>,
   onWarning?: (cause: GateDimensionWarningCause, detail: string) => void,
): GateDimensionResolution | undefined {
   const candidates = findGateDimensionCandidates(struct);
   if (candidates.length > 1) {
      throw new ModelCompilationError({
         message:
            `Source "${sourceName}" declares more than one #(authorize) gate ` +
            `dimension (${candidates.map(gateFieldName).join(", ")}); a ` +
            `source may declare at most one`,
      });
   }

   const field = candidates[0];
   if (!field) {
      // No gate dimension of its own — but a same-named one may have been
      // silently shed: `struct` redefines a base's gate dimension without
      // re-annotating it, which would otherwise ungate the source with no
      // signal at all. Re-declaring WITH the annotation is a legal override
      // (own replaces inherited) and is handled by the `field` branch below,
      // not here; a plain `extend` that never touches the name is also
      // handled below, since Malloy's flattening leaves the identical
      // (still-annotated) field object in `struct.fields`.
      //
      // KNOWN GAP (see task-2-report.md's Concerns): `resolveDeclaredSource`
      // only resolves for a plain, unmodified reference or join — an extend
      // that redefines ANY field (the shape this check exists to catch,
      // since redeclaring the gate dimension's own name requires `except:`
      // first) returns `{kind: "none"}`, confirmed empirically against both
      // an `except:` and a no-op `rename:` extend. Malloy's compiled IR
      // leaves no link from such a struct back to its base at all — no
      // `referenceID`/`sourceID`, no struct-level annotations. This branch
      // is therefore reachable only for the narrower cases where a link DOES
      // exist; it does not fire for the general redefinition shape.
      const declared = resolveDeclaredSource(struct, modelDef);
      if (declared.kind === "resolved") {
         const baseField = findGateDimensionCandidates(declared.source)[0];
         if (baseField) {
            const name = gateFieldName(baseField);
            const current = struct.fields.find(
               (f) => gateFieldName(f) === name,
            );
            if (current && current !== baseField) {
               throw new ModelCompilationError({
                  message:
                     `Source "${sourceName}" redefines the inherited gate ` +
                     `dimension "${name}" without its #(authorize) ` +
                     `annotation. Re-declare the annotation on the ` +
                     `redefinition, or remove the redefinition to inherit ` +
                     `the gate unchanged`,
               });
            }
         }
      }
      return undefined;
   }

   const name = gateFieldName(field);
   // `FieldDef` is a union of dimension/join/view shapes; only the dimension
   // member actually carries `expressionType`/`e`, so this reads through a
   // duck type rather than forcing a `hasExpression`-style narrowing that
   // `@malloydata/malloy`'s root barrel does not export.
   const asExpr = field as unknown as {
      type?: string;
      expressionType?: Parameters<typeof expressionIsScalar>[0];
      e?: unknown;
      accessModifier?: string;
   };
   if (asExpr.accessModifier === "private") {
      throw new ModelCompilationError({
         message:
            `Gate dimension "${name}" on source "${sourceName}" is declared ` +
            `\`private\`, which hides it from the graft that enforces it — ` +
            `the model would load but every query would then fail to compile. ` +
            `Declare it \`internal\` instead`,
      });
   }
   if (
      asExpr.type !== "boolean" ||
      !expressionIsScalar(asExpr.expressionType) ||
      asExpr.e === undefined
   ) {
      throw new ModelCompilationError({
         message:
            `#(authorize) on "${sourceName}.${name}" must annotate a scalar ` +
            `boolean dimension with an expression, not a measure, view, or ` +
            `raw column`,
      });
   }

   const givenIds = expandGivenIds(struct, field);
   for (const id of givenIds) {
      const given = modelDef.givens?.[id];
      if (!given || !declaredGivenNames.has(given.name)) {
         throw new ModelCompilationError({
            message:
               `Gate dimension "${name}" on source "${sourceName}" ` +
               `references a given this model cannot resolve; declare or ` +
               `import it here`,
         });
      }
   }
   for (const id of givenIds) {
      const given = modelDef.givens?.[id];
      if (
         given &&
         (given.default !== undefined || given.defaultText !== undefined)
      ) {
         throw new ModelCompilationError({
            message:
               `Gate dimension "${name}" on source "${sourceName}" ` +
               `references \`$${given.name}\`, which is declared with a ` +
               `default. A caller who supplies no value for \`$${given.name}\` ` +
               `gets that default, which can admit rows the gate was meant ` +
               `to exclude. Declare \`$${given.name}\` with no default`,
         });
      }
   }

   if (givenIds.size === 0) {
      onWarning?.(
         "gate_dimension_no_given_reference",
         `Gate dimension "${name}" on source "${sourceName}" references no ` +
            `given; it is a fixed predicate, not an access rule keyed on ` +
            `the caller`,
      );
   }
   if (containsNegatedMembership(asExpr.e)) {
      onWarning?.(
         "gate_dimension_negated_membership",
         `Gate dimension "${name}" on source "${sourceName}" negates a ` +
            `membership test; an empty given then matches every row instead ` +
            `of none`,
      );
   }

   return { name, field };
}

/**
 * Run {@link validateGateDimension} for every top-level source in
 * `modelDef.contents` (Constraint 3 — entry-point-only; a joined source's own
 * gate dimension is never reached from here since it never appears in
 * `contents` under its own name). The one call both `Model.create`
 * (`service/model.ts`) and the package-load worker make, so the two compile
 * paths validate identically — same pairing as `assertNoMisplacedAuthorizeAnnotations`
 * / `validateAuthorizeProbes` for the string form.
 */
export function validateGateDimensionsForModel(
   modelDef: ModelDef,
   declaredGivenNames: ReadonlySet<string>,
   onWarning?: (
      sourceName: string,
      cause: GateDimensionWarningCause,
      detail: string,
   ) => void,
): void {
   for (const obj of Object.values(modelDef.contents)) {
      if (!isSourceDef(obj)) continue;
      const sourceName = (obj as { as?: string }).as || obj.name;
      validateGateDimension(
         sourceName,
         obj,
         modelDef,
         declaredGivenNames,
         (cause, detail) => onWarning?.(sourceName, cause, detail),
      );
   }
}
