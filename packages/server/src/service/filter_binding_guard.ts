// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Guards against a filter resolving to the WRONG field once a caller frees
 * up the name it was written against (`rename:`, or `except:`/`accept:`
 * dropping the original then a later block reusing the name) — Malloy
 * resolves a filter's field references by name, late, against whatever
 * struct it ends up attached to, and never re-checks that the name still
 * points at what it did when the filter was written. See `docs/authorize.md`.
 *
 * Covers a grafted `#(access_filter)`/`#(authorize)` condition
 * ({@link assertGraftedGateBindsToDeclaringSource}, which resolves the
 * struct whose OWN annotation wrote the note via
 * {@link findAnnotationDeclaringSource}), a plain author `where:` inherited
 * through an `extend` and a joined source's own filters
 * ({@link assertInheritedSourceFiltersBind}, walking
 * {@link nextDerivationLink}'s structural links back to the declaring
 * struct), and `#(filter)` injection (`Model.assertFilterAnnotationsBindToDeclaringSource`
 * in `./model.ts`, via {@link findFilterAnnotationDeclaringSource} and
 * {@link assertFilterDimensionBindsToDeclaringSource}). All three reuse the
 * one field-identity primitive, {@link assertFilterConditionBindsToDeclaringSource}.
 *
 * "Identical field" (this module's one comparison) means: same `name`, same
 * `type`, and a deep-equal `e` — ignoring `annotations` and `accessModifier`
 * by name, and `location`/`at` by VALUE SHAPE (a real `DocumentLocation`,
 * never by key name alone, since both are also legal parameter and
 * record-literal-field names — see {@link isDocumentLocation}), all of which
 * can legitimately differ across a derivation without the field being a
 * different one. A join hop additionally requires the same
 * `join` relationship and a deep-equal `onExpression`, `filterList`,
 * `parameters` and `arguments` on the joined struct, since two same-table
 * sources share a `name` and differ in rows only through those. Source
 * positions are never compared, so a caller who re-joins exactly what the
 * author declared is served, and one who changes what it reads is refused on
 * that change. A field is looked up
 * by its ACTIVE name (`.as` if aliased, else `.name` — see {@link activeName}),
 * never by `.name` alone, so an aliased join member is compared correctly
 * instead of failing to resolve at all.
 *
 * **The default direction is DENY, not pass.** Whenever this module cannot
 * resolve a filter's declaring source, cannot resolve a field along the way,
 * or a resolution walk hits its bound, the caller-facing answer is "cannot
 * prove this binds correctly" — never "must be fine". A condition needs NO
 * check at all only when it is PROVEN to read no field
 * ({@link conditionReadsNoField}) or PROVEN to be the executed struct's own,
 * freshly-authored filter ({@link isOwnFreshFilter}) — never merely because
 * a resolution attempt came up empty.
 */

import {
   isJoined,
   isSourceDef,
   type FieldDef,
   type FilterCondition,
   type ModelDef,
   type SourceDef,
} from "@malloydata/malloy";
import {
   ANCESTOR_WALK_MAX_DEPTH,
   resolveCompositeResolvedBase,
   resolveDeclaredSource,
   resolveQuerySourceBase,
} from "./gate_registry_walk";
import { findSourceByOwnAnnotationIdentity } from "./gate_classification";
import { parseAuthorizeAnnotation } from "./authorize";

/**
 * The next struct back in a derivation chain, trying the same three links
 * `collectEntryPointGatesForRoute`/`ancestorGateExprs` walk for the identical
 * purpose, nearest first: {@link resolveDeclaredSource}'s `sourceRegistry`
 * link (real for a plain join or an unmodified rename), then
 * {@link findSourceByOwnAnnotationIdentity} (a modified derivation that still
 * carries its base's annotation NOTES by reference), then
 * {@link resolveQuerySourceBase} (`query.structRef` — the ONE link
 * `resolveDeclaredSource` never carries, because a `query_source` struct
 * (`Z is X -> {...}`) has no `sourceRegistry` entry of its own; see
 * `gate_registry_walk.ts`'s module doc). Omitting this last link is exactly
 * what let a query-source derivation's gate resolve to "no annotation found"
 * here even though it structurally, and correctly, inherits one.
 */
function nextDerivationLink(
   current: SourceDef,
   modelDef: ModelDef,
   seen: ReadonlySet<SourceDef>,
): SourceDef | undefined {
   const declared = resolveDeclaredSource(current, modelDef);
   if (declared.kind === "resolved" && !seen.has(declared.source)) {
      return declared.source;
   }
   if (declared.kind === "none") {
      const viaIdentity = findSourceByOwnAnnotationIdentity(
         current,
         modelDef,
         new Set(seen),
      );
      if (viaIdentity && !seen.has(viaIdentity)) return viaIdentity;
      const viaQuerySource = resolveQuerySourceBase(current, modelDef);
      if (viaQuerySource && !seen.has(viaQuerySource)) return viaQuerySource;
   }
   return undefined;
}

/**
 * Duck-typed rather than imported: Malloy does not re-export `RefSummary`
 * from its package root (the same situation `authorize.ts`'s
 * `CompiledGateCondition` doc describes). Structurally compatible with the
 * real `FilterCondition.refSummary` / `FieldDef.refSummary`, so no cast is
 * needed at a call site that already has one of those.
 */
interface FieldUsageEntryLike {
   path: readonly string[];
}
interface RefSummaryLike {
   fieldUsage?: readonly FieldUsageEntryLike[];
}

/** Bounds the transitive field-usage walk ({@link fieldUsageClosure}) and the
 *  joined-source recursion ({@link assertInheritedSourceFiltersBind}) — large
 *  enough for any real model, small enough that a pathological/cyclic one
 *  fails closed instead of hanging. Exported so a depth-exhaustion test can
 *  reference the real bound rather than hardcoding a number that could drift. */
const MAX_CLOSURE_SIZE = 256;
export const MAX_JOIN_RECURSION_DEPTH = 16;

/** The name a field goes by in ITS CURRENT context: its `as` alias when it
 *  has one, otherwise its intrinsic `name` — same rule Malloy's own
 *  (unexported-from-package-root) `activeName` uses, matched here rather
 *  than imported (`get_context_tool.ts` keeps an identical local copy for
 *  the same reason). A `refSummary.fieldUsage` path segment is always the
 *  VISIBLE name — a join declared under an alias (`join_one: child is
 *  real_source on …`) carries `real_source`'s own `.name` with `.as:
 *  "child"` on the join field itself, so matching on `.name` alone misses
 *  every aliased join and falsely denies a query that never misbound
 *  anything. */
function activeName(f: { name: string; as?: string }): string {
   return f.as ?? f.name;
}

/** Stripped everywhere by NAME: neither collides with a name-keyed record
 *  (`parameters`/`arguments`/a record-literal's `kids`) anywhere in the IR. */
const IGNORED_KEYS_BY_NAME = new Set(["annotations", "accessModifier"]);

/** Stripped only when the VALUE is actually a `DocumentLocation` — `location`
 *  and `at` are also legal author parameter/record-literal-field names
 *  (`SafeRecord<Parameter|Argument>` and `RecordLiteralNode.kids` are both
 *  keyed by the author's own names), so stripping by key name alone deletes
 *  those entries as if they were position metadata and makes two different
 *  bindings compare equal. */
const SHAPE_CHECKED_KEYS = new Set(["location", "at"]);

/** Structural match for `DocumentLocation` (`{url, range: {start, end}}`,
 *  each a `{line, character}`) — the one shape `location`/`at` take as real
 *  IR metadata; no Expr node or Parameter value can match it. */
function isDocumentLocation(v: unknown): boolean {
   if (!v || typeof v !== "object") return false;
   const o = v as Record<string, unknown>;
   if (typeof o.url !== "string" || !o.range || typeof o.range !== "object") {
      return false;
   }
   const isPosition = (p: unknown): boolean =>
      !!p &&
      typeof p === "object" &&
      typeof (p as Record<string, unknown>).line === "number" &&
      typeof (p as Record<string, unknown>).character === "number";
   const range = o.range as Record<string, unknown>;
   return isPosition(range.start) && isPosition(range.end);
}

/** Joined-struct properties, beyond `name`, that decide which rows the join reaches. */
const JOINED_STRUCT_ROW_KEYS = ["filterList", "parameters", "arguments"];

/** {@link JOINED_STRUCT_ROW_KEYS} entries that are name-keyed records
 *  (`SafeRecord<Parameter|Argument>`), compared entry-by-entry so a
 *  parameter literally named `location`/`at`/`annotations`/`accessModifier`
 *  keeps its own identity instead of being merged under {@link strip}'s
 *  generic key-based pass. `filterList` is an array, not a name-keyed
 *  record, so it keeps going through {@link deepEqualIgnoring} as-is. */
const NAME_KEYED_RECORD_KEYS = new Set(["parameters", "arguments"]);

/** Structural equality ignoring {@link IGNORED_KEYS_BY_NAME} and
 *  {@link SHAPE_CHECKED_KEYS} (by shape). A key-order mismatch between two
 *  otherwise-identical objects would read as "different" here — that fails
 *  CLOSED (an extra denial), never open, so it is not chased. */
function deepEqualIgnoring(a: unknown, b: unknown): boolean {
   return JSON.stringify(strip(a)) === JSON.stringify(strip(b));
}

/** Entry-by-entry equality for a `SafeRecord` keyed by the author's own
 *  names (`parameters`/`arguments`) — never deletes an entry by its key, so
 *  a binding named `location`/`at`/etc. is compared like any other. */
function recordEntriesEqual(a: unknown, b: unknown): boolean {
   const ao = (a && typeof a === "object" ? a : {}) as Record<string, unknown>;
   const bo = (b && typeof b === "object" ? b : {}) as Record<string, unknown>;
   const aKeys = Object.keys(ao);
   const bKeys = Object.keys(bo);
   if (aKeys.length !== bKeys.length) return false;
   return aKeys.every((k) => k in bo && deepEqualIgnoring(ao[k], bo[k]));
}

function strip(value: unknown): unknown {
   if (Array.isArray(value)) return value.map(strip);
   if (value && typeof value === "object") {
      const obj = value as Record<string, unknown>;
      const out: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(obj)) {
         // A record literal's `kids` is a `SafeRecord<Expr>` keyed by the
         // author's own field names — same trap as `parameters`/`arguments`,
         // but reached through this generic recursion rather than the
         // `JOINED_STRUCT_ROW_KEYS` call site, so it needs its own guard here.
         if (k === "kids" && obj.node === "recordLiteral") {
            const kids = (v && typeof v === "object" ? v : {}) as Record<
               string,
               unknown
            >;
            out[k] = Object.fromEntries(
               Object.entries(kids).map(([kk, kv]) => [kk, strip(kv)]),
            );
            continue;
         }
         if (IGNORED_KEYS_BY_NAME.has(k)) continue;
         if (SHAPE_CHECKED_KEYS.has(k) && isDocumentLocation(v)) continue;
         out[k] = strip(v);
      }
      return out;
   }
   return value;
}

/** Whether `a` and `b` are the "same" field per this module's definition —
 *  see the module doc. `undefined` on either side (the name didn't resolve)
 *  is never identical: a caller checking a fieldUsage path already confirmed
 *  it resolves in the DECLARING struct, so a lookup miss on the executed
 *  struct means the field genuinely moved. */
function fieldsIdentical(
   a: FieldDef | undefined,
   b: FieldDef | undefined,
): boolean {
   if (!a || !b) return false;
   if (a.name !== b.name || a.type !== b.type) return false;
   if (!deepEqualIgnoring((a as { e?: unknown }).e, (b as { e?: unknown }).e))
      return false;
   if (isJoined(a) || isJoined(b)) {
      if (!isJoined(a) || !isJoined(b)) return false;
      if (a.join !== b.join) return false;
      if (!deepEqualIgnoring(a.onExpression, b.onExpression)) return false;
      // A same-table sibling (`where:`, or different parameter bindings) keeps the joined struct's `name`.
      const ja = a as unknown as Record<string, unknown>;
      const jb = b as unknown as Record<string, unknown>;
      for (const key of JOINED_STRUCT_ROW_KEYS) {
         const equal = NAME_KEYED_RECORD_KEYS.has(key)
            ? recordEntriesEqual(ja[key], jb[key])
            : deepEqualIgnoring(ja[key], jb[key]);
         if (!equal) return false;
      }
   }
   return true;
}

/** Whether an expression tree contains a field reference anywhere. */
function expressionReadsField(e: unknown): boolean {
   if (Array.isArray(e)) return e.some(expressionReadsField);
   if (!e || typeof e !== "object") return false;
   if ((e as { node?: unknown }).node === "field") return true;
   return Object.values(e as Record<string, unknown>).some(
      expressionReadsField,
   );
}

/** Resolve a dotted field path (a join path, for a field reached through one
 *  or more joins) against one struct's own `fields`. `undefined` if any
 *  segment is missing, or an intermediate segment is not itself a join. */
function resolveFieldByPath(
   struct: SourceDef | undefined,
   path: readonly string[],
): FieldDef | undefined {
   let current: SourceDef | undefined = struct;
   let field: FieldDef | undefined;
   for (let i = 0; i < path.length; i++) {
      if (!current) return undefined;
      field = current.fields?.find((f) => activeName(f) === path[i]);
      if (!field) return undefined;
      if (i < path.length - 1) {
         if (!isJoined(field)) return undefined;
         current = field as unknown as SourceDef;
      }
   }
   return field;
}

/**
 * Whether `path` resolves to the same field — see the module doc — in
 * `declaring` as it does in `executed`. Every hop along a join path is
 * checked, not just the leaf: a join swapped for a differently-related one
 * partway down the path is exactly the kind of divergence this exists to
 * catch, even when the LEAF field it ends on happens to match.
 */
function fieldPathIdentical(
   declaring: SourceDef,
   executed: SourceDef,
   path: readonly string[],
): boolean {
   let dCur: SourceDef | undefined = declaring;
   let eCur: SourceDef | undefined = executed;
   for (let i = 0; i < path.length; i++) {
      const name = path[i];
      const dField = dCur?.fields?.find((f) => activeName(f) === name);
      const eField = eCur?.fields?.find((f) => activeName(f) === name);
      if (!fieldsIdentical(dField, eField)) return false;
      if (i < path.length - 1) {
         if (!dField || !isJoined(dField) || !eField || !isJoined(eField)) {
            return false;
         }
         dCur = dField as unknown as SourceDef;
         eCur = eField as unknown as SourceDef;
      }
   }
   return true;
}

/**
 * Every field-usage path reachable from `refSummary`, resolved TRANSITIVELY
 * through `declaring`. Three kinds of dependency are followed, each one hop at
 * a time:
 * - a field's own expression: a filter that reads a dimension (`org_id in
 *   $GROUPS` over `#(access_filter) authorized`, say) only lists `authorized`
 *   in its own `fieldUsage`, so the fields THAT dimension reads come from its
 *   own `refSummary`;
 * - each join a path goes through: the fields its ON (or `with`) reads,
 *   relative to the struct that declares the join;
 * - each such join's joined source: the fields its own `where:` conditions
 *   read, relative to the joined struct.
 * Returns `truncated: true` (never a partial list) when the walk would exceed
 * {@link MAX_CLOSURE_SIZE}, and `unrecordedJoin` naming a join whose ON, or
 * one of its joined source's `where:` conditions, reads fields but carries no
 * `refSummary` to list them. The caller must treat
 * either as "cannot prove this binds correctly" (deny), not "here is
 * everything there is".
 */
function fieldUsageClosure(
   declaring: SourceDef,
   refSummary: RefSummaryLike | undefined,
): { paths: string[][]; truncated: boolean; unrecordedJoin?: string } {
   const seen = new Set<string>();
   const paths: string[][] = [];
   const queue: string[][] = (refSummary?.fieldUsage ?? []).map(
      (u: FieldUsageEntryLike) => [...u.path],
   );
   while (queue.length > 0) {
      if (paths.length >= MAX_CLOSURE_SIZE) return { paths, truncated: true };
      const path = queue.shift();
      if (!path) break;
      const key = path.join("\u0000");
      if (seen.has(key)) continue;
      seen.add(key);
      paths.push(path);
      const field = resolveFieldByPath(declaring, path);
      const nested = (field as { refSummary?: RefSummaryLike } | undefined)
         ?.refSummary;
      // A nested `FieldUsageEntry.path` is relative to the struct `field`
      // ITSELF lives on (`path`'s join prefix, i.e. everything but its own
      // last segment) — never relative to `declaring` directly. A field
      // reached through a join (`path = ["childtable", "name"]`) whose own
      // nested usage is a bare self-reference (`u.path = ["name"]`, which a
      // plain physical column can carry trivially) would otherwise queue as
      // `["name"]` alone: unresolvable against `declaring` (there is no
      // top-level "name" on `parent`), which reads as "the field moved"
      // rather than "this was never `declaring`-relative to begin with".
      // Re-qualifying against `path`'s own prefix keeps every queued entry
      // in the same coordinate space `resolveFieldByPath`/`fieldPathIdentical`
      // already assume everywhere else in this module.
      const prefix = path.slice(0, -1);
      for (const u of nested?.fieldUsage ?? []) {
         queue.push([...prefix, ...u.path]);
      }
      // Each join on the way also reads its ON (relative to the struct that
      // declares it) and its own `where:` (relative to the joined struct):
      // rebinding either moves the row the path reaches without touching it.
      for (let i = 0; i < path.length - 1; i++) {
         const join = resolveFieldByPath(declaring, path.slice(0, i + 1)) as
            | {
                 onExpression?: unknown;
                 refSummary?: RefSummaryLike;
                 filterList?: readonly {
                    refSummary?: RefSummaryLike;
                    e?: unknown;
                 }[];
              }
            | undefined;
         if (
            join &&
            !join.refSummary &&
            expressionReadsField(join.onExpression)
         ) {
            return {
               paths,
               truncated: false,
               unrecordedJoin: path.slice(0, i + 1).join("."),
            };
         }
         for (const u of join?.refSummary?.fieldUsage ?? []) {
            queue.push([...path.slice(0, i), ...u.path]);
         }
         for (const condition of join?.filterList ?? []) {
            if (!condition.refSummary && expressionReadsField(condition.e)) {
               return {
                  paths,
                  truncated: false,
                  unrecordedJoin: path.slice(0, i + 1).join("."),
               };
            }
            for (const u of condition.refSummary?.fieldUsage ?? []) {
               queue.push([...path.slice(0, i + 1), ...u.path]);
            }
         }
      }
   }
   return { paths, truncated: false };
}

function isCompositePlaceholder(field: FieldDef): boolean {
   return (field as { e?: { node?: unknown } }).e?.node === "compositeField";
}

/** Structural, not by identity: the declaring composite can come from a
 *  sibling model's compile. Every member condition must still apply, so the
 *  executed rows never exceed the member's. */
function couldExecuteAsMember(member: SourceDef, executed: SourceDef): boolean {
   if (member.name !== executed.name) return false;
   const m = member as unknown as Record<string, unknown>;
   const e = executed as unknown as Record<string, unknown>;
   if (!recordEntriesEqual(m.parameters, e.parameters)) return false;
   if (!recordEntriesEqual(m.arguments, e.arguments)) return false;
   const applied = (executed.filterList ?? []).map((c) =>
      JSON.stringify(strip(c)),
   );
   return (member.filterList ?? []).every((c) =>
      applied.includes(JSON.stringify(strip(c))),
   );
}

function candidateMembers(
   composite: SourceDef,
   executed: SourceDef,
   out: SourceDef[] = [],
   depth = 0,
): SourceDef[] {
   if (depth > MAX_JOIN_RECURSION_DEPTH) return out;
   const members = (composite as { sources?: readonly SourceDef[] }).sources;
   for (const member of members ?? []) {
      if ((member as { type: string }).type === "composite") {
         candidateMembers(member, executed, out, depth + 1);
      } else if (couldExecuteAsMember(member, executed)) {
         out.push(member);
      }
   }
   return out;
}

function memberDeclaringCondition(
   composite: SourceDef,
   executed: SourceDef,
   condition: FilterCondition,
): SourceDef | undefined {
   const written = JSON.stringify(strip(condition));
   return candidateMembers(composite, executed).find((member) =>
      (member.filterList ?? []).some(
         (own) => JSON.stringify(strip(own)) === written,
      ),
   );
}

/** A placeholder becomes the candidate member's own field only when the
 *  executed field is identical to it; otherwise it stays, and fails the
 *  comparison. */
function withCompositeMembersResolved(
   declaring: SourceDef,
   executed: SourceDef,
): SourceDef {
   if ((declaring as { type: string }).type !== "composite") return declaring;
   const members = candidateMembers(declaring, executed);
   if (members.length === 0) return declaring;
   let resolved = false;
   const fields = (declaring.fields ?? []).map((field) => {
      if (!isCompositePlaceholder(field)) return field;
      const name = activeName(field);
      const executedField = executed.fields?.find(
         (f) => activeName(f) === name,
      );
      for (const member of members) {
         const own = member.fields?.find((f) => activeName(f) === name);
         if (own && fieldsIdentical(own, executedField)) {
            resolved = true;
            return own;
         }
      }
      return field;
   });
   return resolved ? { ...declaring, fields } : declaring;
}

/**
 * Assert that `condition` (declared, or landed, against `declaringStruct`)
 * still reads the SAME fields when evaluated against `executedStruct`. Throws
 * a plain `Error` — never `AccessDeniedError` — so every call site decides
 * its own caller-facing message; the caller is expected to catch and deny.
 */
export function assertFilterConditionBindsToDeclaringSource(
   declaringStructAsWritten: SourceDef,
   executedStruct: SourceDef,
   condition: FilterCondition,
): void {
   const declaringStruct = withCompositeMembersResolved(
      declaringStructAsWritten,
      executedStruct,
   );
   const { paths, truncated, unrecordedJoin } = fieldUsageClosure(
      declaringStruct,
      condition.refSummary,
   );
   if (truncated) {
      throw new Error(
         "a row-security filter's field-usage closure exceeded the resolution bound",
      );
   }
   if (unrecordedJoin) {
      throw new Error(
         `a row-security filter reaches through \`${unrecordedJoin}\`, whose ON or where: inputs are not recorded`,
      );
   }
   for (const path of paths) {
      if (!fieldPathIdentical(declaringStruct, executedStruct, path)) {
         throw new Error(
            `a row-security filter references \`${path.join(".")}\`, which resolves to a different field on the executed source`,
         );
      }
   }
}

/**
 * The `#(filter)` analogue of {@link assertFilterConditionBindsToDeclaringSource}
 * for a bare dimension NAME rather than a compiled `FilterCondition` — there
 * is no condition object here to read a `refSummary` off directly, since the
 * value being injected is a runtime parameter, not a stored expression.
 * Seeds {@link fieldUsageClosure} with a single top-level path (`[dimension]`)
 * and walks its transitive closure exactly the same way: a dimension defined
 * as an alias of another field (`dimension: safe_col is val`) has its OWN
 * `.e` unchanged by a caller renaming `val` out from under it, so comparing
 * only `[dimension]` at the top level sees no difference — the closure is
 * what follows into `val` itself and catches the rebind.
 */
export function assertFilterDimensionBindsToDeclaringSource(
   declaringStruct: SourceDef,
   executedStruct: SourceDef,
   dimension: string,
): void {
   const { paths, truncated, unrecordedJoin } = fieldUsageClosure(
      declaringStruct,
      { fieldUsage: [{ path: [dimension] }] },
   );
   if (truncated) {
      throw new Error(
         "a #(filter) dimension's field-usage closure exceeded the resolution bound",
      );
   }
   if (unrecordedJoin) {
      throw new Error(
         `a #(filter) dimension reaches through \`${unrecordedJoin}\`, whose ON or where: inputs are not recorded`,
      );
   }
   for (const path of paths) {
      if (!fieldPathIdentical(declaringStruct, executedStruct, path)) {
         throw new Error(
            `a #(filter) dimension references \`${path.join(".")}\`, which resolves to a different field on the executed source`,
         );
      }
   }
}

/**
 * Whether `a` sits strictly before `b` in a document (line first, then
 * character) — mirrors `source_extraction.ts`'s identical helper (kept
 * self-contained here rather than imported: that module is bundled into the
 * package-load worker and stays free of anything not needed there).
 */
function isEarlierPosition(
   a: { line: number; character: number },
   b: { line: number; character: number },
): boolean {
   return a.line < b.line || (a.line === b.line && a.character < b.character);
}

/** Duck-typed `Note`/`AnnotationsDef` — same reason every other `*Like`
 *  interface in this module is: no need to import Malloy's real types just
 *  to read a few fields off something a caller already has typed loosely. */
interface NoteLike {
   text: string;
   at?: DocumentLocationLike;
}
interface AnnotationsDefLike {
   blockNotes?: readonly NoteLike[];
   notes?: readonly NoteLike[];
   inherits?: AnnotationsDefLike;
}

/** Every note `annotations` carries at EVERY level of its `inherits` chain,
 *  own-level first — not just the top one. Malloy demotes a struct's
 *  inherited annotations to `annotations.inherits` the moment the struct
 *  declares ANY annotation of its own (own-level, ANY kind, not only
 *  `#(access_filter)`/`#(authorize)`/`#(filter)` — see `gate_registry_walk.ts`'s
 *  module doc for the general rule), so a derived source's OWN top-level
 *  notes can be missing the very note that produced a filter it merely
 *  inherited. Reading only top-level notes then makes {@link findNoteOwner}
 *  search for the WRONG note (or nothing), and can land on the derived
 *  struct itself rather than the true declaring ancestor. Bounded the same
 *  as every other ancestor walk here, for the same reason: a resolver that
 *  can loop forever on malformed IR is worse than one that gives up. */
function allAnnotationNotes(
   annotations: AnnotationsDefLike | undefined,
   depth = 0,
): NoteLike[] {
   if (!annotations || depth > ANCESTOR_WALK_MAX_DEPTH) return [];
   return [
      ...(annotations.blockNotes ?? []),
      ...(annotations.notes ?? []),
      ...allAnnotationNotes(annotations.inherits, depth + 1),
   ];
}

/** A struct's OWN-level notes only — never its `inherits` chain. This is
 *  the "did THIS struct itself write this note" test {@link findNoteOwner}
 *  needs for each candidate; widening it to the chain would make every
 *  inheriting derivation match too, which is exactly the ambiguity
 *  {@link findNoteOwner}'s doc warns about. */
function ownLevelNotes(
   annotations: AnnotationsDefLike | undefined,
): NoteLike[] {
   return [...(annotations?.blockNotes ?? []), ...(annotations?.notes ?? [])];
}

/**
 * The top-level struct that ACTUALLY WROTE one of `notes` — by LOCATION,
 * never by object identity or `sourceRegistry`. Mirrors
 * `source_extraction.ts`'s `considerNoteOwner`, kept as its own compact copy
 * for the same reason `isEarlierPosition` above is.
 *
 * Location, not identity: a shared-note identity check can't tell the true
 * declarer from a fellow inheritor sharing the same note by reference, and
 * can land on a sibling derivation instead — denying a perfectly ungated
 * query. Own-level notes only ({@link ownLevelNotes}), never `inherits`, for
 * the same reason.
 *
 * Resolves EACH note independently and returns on the first one that finds an
 * owner, rather than accumulating one "best" candidate across every note in
 * `notes`: two different notes can own-resolve in two different FILES, and
 * comparing their candidates' raw line/char positions against each other is
 * meaningless — it is only ever safe to compare positions among candidates
 * for the SAME note, which (via the `url` check below) already share a file.
 */
function findNoteOwner(
   notes: readonly NoteLike[],
   modelDef: ModelDef,
): SourceDef | undefined {
   for (const note of notes) {
      if (!note.at) continue;
      let best: SourceDef | undefined;
      for (const value of Object.values(modelDef.contents)) {
         if (!isSourceDef(value) || !value.location) continue;
         if (!ownLevelNotes(value.annotations).includes(note)) continue;
         if (value.location.url !== note.at.url) continue;
         if (
            !best?.location ||
            isEarlierPosition(
               value.location.range.start,
               best.location.range.start,
            )
         ) {
            best = value;
         }
      }
      if (best) return best;
   }
   return undefined;
}

/**
 * Whether `note` parses as an `#(access_filter)`/`#(authorize)` gate
 * annotation — the only kind {@link findAnnotationDeclaringSource}'s owner
 * search may ever consider. A struct commonly carries OTHER notes too (a
 * render tag, `#(doc)`), and feeding those into {@link findNoteOwner}
 * alongside the real gate note is exactly the bug this guards against: a
 * derived source's own unrelated tag, sitting at a lower line number than the
 * ancestor that actually wrote the gate, could otherwise resolve as its own
 * "owner" and make a cross-file misbind look self-contained. A malformed
 * annotation (an empty body) is treated as not-a-gate-note here rather than
 * thrown — this is candidate FILTERING, not the gate's own load-time
 * validation, which denies malformed gates elsewhere.
 */
function isGateNote(note: NoteLike): boolean {
   try {
      return parseAuthorizeAnnotation(note.text) !== null;
   } catch {
      return false;
   }
}

/**
 * {@link findNoteOwner} over the gate notes `struct` carries, own-level plus
 * `inherits` (see {@link allAnnotationNotes}), NARROWED to
 * {@link isGateNote} — a render tag or `#(doc)` note must never enter this
 * search, since it carries no ownership information about the actual gate
 * and would otherwise get compared against the real gate note's owner
 * candidates as if it were an alternative resolution for the SAME thing.
 * Falls back to the structural walk {@link findFilterOrigin} uses
 * ({@link nextDerivationLink}) when the note search is empty — needed for a
 * `query_source` (`Z is X -> {...}`), which carries no `annotations` at all
 * despite genuinely inheriting `X`'s gate (see `gate_registry_walk.ts`'s
 * module doc). `resolveSibling` is the last resort after both fail — see
 * {@link findSiblingDeclaringSource}.
 */
function findAnnotationDeclaringSource(
   struct: SourceDef,
   modelDef: ModelDef,
   resolveSibling?: SiblingModelDefResolver,
): SourceDef | undefined {
   const notes = allAnnotationNotes(struct.annotations).filter(isGateNote);
   const viaNotes = findNoteOwner(notes, modelDef);
   if (viaNotes) return viaNotes;
   let current = struct;
   const seen = new Set<SourceDef>([struct]);
   for (let depth = 0; depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      const next = nextDerivationLink(current, modelDef, seen);
      if (!next) break;
      if (ownLevelNotes(next.annotations).some(isGateNote)) return next;
      seen.add(next);
      current = next;
   }
   for (const note of notes) {
      const sibling = findSiblingAnnotationDeclaringSource(
         note,
         resolveSibling,
      );
      if (sibling) return sibling;
   }
   // Last resort: a composite entry point (`comp is compose(a, b) -> {...}`)
   // holds the gate note on neither `struct.annotations` nor anything
   // `nextDerivationLink`'s three links (built for extend/rename, never
   // composite-member resolution) can reach from `struct` — Malloy resolves
   // the composite to exactly ONE concrete member for THIS derivation and
   // reads THAT member's own annotations during entry-point-gate collection
   // (`gate_classification.ts`), never copying the note onto `struct` itself.
   // {@link findCompositeMemberDeclaringSource} is the one place that member
   // is actually reachable from here.
   return findCompositeMemberDeclaringSource(struct, modelDef);
}

/**
 * The declaring struct for a gate note reachable ONLY through `struct`'s OWN
 * composite-member resolution — see {@link findAnnotationDeclaringSource}'s
 * last-resort call. `resolveCompositeResolvedBase` names the exact composite
 * member Malloy resolved `struct`'s query against (`query.compositeResolvedSourceDef`,
 * set only when `struct` is a `query_source` over a composite base); `undefined`
 * for anything else, so this is a no-op for a non-composite entry point.
 * Once that member is in hand, this is the EXACT SAME two-step search
 * `findAnnotationDeclaringSource` itself runs — own gate notes, then the
 * structural walk — seeded at the member instead of `struct`, since the
 * member can itself be a further derivation (`member extend {...}`) rather
 * than the note's direct owner.
 */
function findCompositeMemberDeclaringSource(
   struct: SourceDef,
   modelDef: ModelDef,
): SourceDef | undefined {
   const member = resolveCompositeResolvedBase(struct);
   if (!member) return undefined;
   if (ownLevelNotes(member.annotations).some(isGateNote)) return member;
   let current = member;
   const seen = new Set<SourceDef>([member]);
   for (let depth = 0; depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      const next = nextDerivationLink(current, modelDef, seen);
      if (!next) break;
      if (ownLevelNotes(next.annotations).some(isGateNote)) return next;
      seen.add(next);
      current = next;
   }
   return undefined;
}

/**
 * {@link findNoteOwner} narrowed to the ONE note that produced `filter` — the
 * `#(filter)` analogue of {@link findAnnotationDeclaringSource}. Needed
 * because `filterMap` (`source_extraction.ts`) keys an entry under every
 * INHERITING derivation's own name too, so `getFilters` can hand back a
 * derived, possibly-misbound source's own name as "the declaring source" —
 * comparing it to itself would always look identical. Matches notes by
 * PARSED definition, not object identity, since the caller only has the
 * parsed `FilterDefinition`, not the original `Note`. Falls back to the
 * structural walk and then `resolveSibling`, same as
 * {@link findAnnotationDeclaringSource}.
 */
export function findFilterAnnotationDeclaringSource(
   struct: SourceDef,
   modelDef: ModelDef,
   filter: {
      readonly name: string;
      readonly dimension: string;
      readonly type: string;
   },
   parseAnnotation: (text: string) => {
      name: string;
      dimension: string;
      type: string;
   } | null,
   resolveSibling?: SiblingModelDefResolver,
): SourceDef | undefined {
   const matches = (notes: readonly NoteLike[]) =>
      notes.filter((note) => {
         let parsed: { name: string; dimension: string; type: string } | null;
         try {
            parsed = parseAnnotation(note.text);
         } catch {
            return false;
         }
         return (
            !!parsed &&
            parsed.dimension === filter.dimension &&
            parsed.type === filter.type &&
            parsed.name === filter.name
         );
      });
   const matched = matches(allAnnotationNotes(struct.annotations));
   const viaNotes = findNoteOwner(matched, modelDef);
   if (viaNotes) return viaNotes;
   let current = struct;
   const seen = new Set<SourceDef>([struct]);
   for (let depth = 0; depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      const next = nextDerivationLink(current, modelDef, seen);
      if (!next) break;
      if (matches(ownLevelNotes(next.annotations)).length > 0) return next;
      seen.add(next);
      current = next;
   }
   for (const note of matched) {
      const sibling = findSiblingAnnotationDeclaringSource(
         note,
         resolveSibling,
      );
      if (sibling) return sibling;
   }
   return undefined;
}

/**
 * Assert that a GRAFTED `#(access_filter)`/`#(authorize)` condition still
 * binds to the same fields at `executedStruct` (the entry point actually
 * queried) as it did at the struct whose OWN annotation wrote it.
 *
 * Unlike a plain author `where:`, a grafted condition is not the same object
 * copied down an `extend` chain — {@link resolveGraftTarget} compiles the
 * annotation's filter text fresh onto the entry point's own field space, so
 * there is no shared object to walk back through. The declaring struct is
 * instead found by {@link findAnnotationDeclaringSource}, by NOTE identity.
 *
 * Always runs the comparison, even when nothing was derived: a fresh compile
 * allocates its own struct instances, so `entryPointStruct` and
 * `executedStruct` are essentially never reference-equal even in the
 * unmodified case — a shortcut permissive enough to fire there would also
 * skip the misbindable case this exists to catch (`resolveGraftTarget`
 * plants the condition on an ancestor of the actual run target whenever the
 * caller's own derivation has no `modelDef.contents` entry of its own).
 *
 * THROWS when `entryPointStruct` itself or its declaring struct cannot be
 * resolved — an unprovable gate denies, it is never treated as nothing to
 * check.
 */
export function assertGraftedGateBindsToDeclaringSource(
   entryPointStruct: SourceDef | undefined,
   executedStruct: SourceDef,
   condition: FilterCondition,
   modelDef: ModelDef,
   resolveSibling?: SiblingModelDefResolver,
): void {
   if (!entryPointStruct) {
      throw new Error(
         "a row-security gate's entry point could not be resolved",
      );
   }
   const declaring = findAnnotationDeclaringSource(
      entryPointStruct,
      modelDef,
      resolveSibling,
   );
   if (!declaring) {
      throw new Error(
         "a row-security gate's declaring source could not be resolved",
      );
   }
   assertFilterConditionBindsToDeclaringSource(
      declaring,
      executedStruct,
      condition,
   );
}

/** Loose structural shape of `DocumentLocation` — duck-typed for the same
 *  reason {@link RefSummaryLike} is: no need to import it just to read two
 *  fields off something a caller already has typed loosely. */
export interface DocumentLocationLike {
   url: string;
   range: {
      start: { line: number; character: number };
      end: { line: number; character: number };
   };
}

/** Whether two ranges mark the exact same span — compared field by field,
 *  never via `JSON.stringify` (key order on the two objects is not
 *  guaranteed to match just because the values do). */
function rangesEqual(
   a: DocumentLocationLike["range"],
   b: DocumentLocationLike["range"],
): boolean {
   return (
      a.start.line === b.start.line &&
      a.start.character === b.start.character &&
      a.end.line === b.end.line &&
      a.end.character === b.end.character
   );
}

/** Whether `outer` fully contains `inner` — same file, `inner` starting no
 *  earlier and ending no later. */
function locationContains(
   outer: DocumentLocationLike,
   inner: DocumentLocationLike,
): boolean {
   if (outer.url !== inner.url) return false;
   if (isEarlierPosition(inner.range.start, outer.range.start)) return false;
   if (isEarlierPosition(outer.range.end, inner.range.end)) return false;
   return true;
}

/** A rough, same-file-only span size — large enough that line differences
 *  always dominate character ones, which is all {@link findConditionOriginByLocation}
 *  needs to prefer the narrowest (most specific) containing struct over a
 *  wider ancestor that also happens to contain the same position. */
function spanSize(range: DocumentLocationLike["range"]): number {
   return (
      (range.end.line - range.start.line) * 100_000 +
      (range.end.character - range.start.character)
   );
}

/**
 * The struct whose OWN source span CONTAINS `at` — used only as a last
 * resort, when a filter's origin cannot be found any other way: a plain,
 * UNANNOTATED `where:` inherited through a MODIFIED derivation has no
 * surviving `referenceID`/`sourceRegistry` link (cleared on modification,
 * see {@link resolveDeclaredSource}) and no annotation note to
 * identity-match through (see {@link findSourceByOwnAnnotationIdentity}) —
 * so there is nothing structural left to walk. But a field reference INSIDE
 * a condition's own compiled expression still carries the location it was
 * PARSED at (`{node:"field", path, at}`), however many times the condition
 * object itself is later copied unchanged down an `extend` chain — the same
 * fact {@link findAnnotationDeclaringSource} relies on for annotation notes,
 * applied here to a plain filter expression instead. That position sits
 * INSIDE the `source:` block that authored it (a `where:` is textually
 * nested in its enclosing source), so the struct whose own `.location` span
 * contains it is the declaring struct — the narrowest (smallest) containing
 * span wins, in case one source's span is textually nested inside another's.
 *
 * Guarded by the caller's `next.filterList?.includes(condition)` check same
 * as every other candidate here: a location match that turns out not to
 * actually own this condition object is rejected there, not accepted on
 * position alone.
 */
function findConditionOriginByLocation(
   modelDef: ModelDef,
   at: DocumentLocationLike,
): SourceDef | undefined {
   let best: SourceDef | undefined;
   let bestSpan = Infinity;
   for (const value of Object.values(modelDef.contents)) {
      if (!isSourceDef(value) || !value.location) continue;
      if (!locationContains(value.location as DocumentLocationLike, at)) {
         continue;
      }
      const span = spanSize(value.location.range);
      if (span < bestSpan) {
         best = value;
         bestSpan = span;
      }
   }
   return best;
}

/**
 * Resolves the `ModelDef` the PACKAGE independently compiled for the file
 * named by a `file://` URL — every `.malloy`/`.malloynb` file a package
 * serves is its own top-level `Model` (see `Package`'s model discovery),
 * regardless of whether anything else imports it, and regardless of whether
 * the SERVED model promoted it to a `modelDef.contents` entry of its own. A
 * selective `import { name } from "file"` or a notebook cell's narrower
 * per-cell compile can leave an ancestor file with no such entry, which is
 * exactly when `modelDef.contents`-based resolution above comes up empty.
 * Wired up by `Package.applySiblingModelResolverToModels` (a Model held
 * outside a Package has none, and this simply always misses for it — the
 * same conservative "cannot prove, so deny" as before this existed).
 */
export type SiblingModelDefResolver = (url: string) => ModelDef | undefined;

/**
 * The declaring struct for a plain filter CONDITION's field-usage location
 * `at`, via the sibling compile, when nothing in the SERVED model's own
 * `modelDef.contents` covers it. Reuses
 * {@link findConditionOriginByLocation}'s span-containment test against the
 * sibling's `ModelDef` — same test, different compile: a condition's field
 * reference is textually nested INSIDE the `source:` block that wrote it,
 * same as it is in the served compile. The binding check that follows this
 * doesn't care which compile produced the struct (it compares fields
 * structurally, not by object identity), so a wrong candidate simply fails
 * to bind rather than being trusted on identity alone; there is no
 * reference-identity re-check to relax here the way {@link findFilterOrigin}'s
 * own walk needs one.
 */
function findSiblingDeclaringSource(
   at: DocumentLocationLike | undefined,
   resolveSibling: SiblingModelDefResolver | undefined,
): SourceDef | undefined {
   if (!at || !resolveSibling) return undefined;
   const siblingModelDef = resolveSibling(at.url);
   if (!siblingModelDef) return undefined;
   return findConditionOriginByLocation(siblingModelDef, at);
}

/**
 * The declaring struct for an ANNOTATION note, via the sibling compile —
 * `findAnnotationDeclaringSource`/`findFilterAnnotationDeclaringSource`'s own
 * analogue of {@link findSiblingDeclaringSource}. A note's `.at` sits BEFORE
 * the `source:` keyword it annotates, not inside the struct's own location
 * span, so {@link findConditionOriginByLocation}'s containment test does not
 * apply here the way it does for a condition's field reference — and the
 * served-model scan's own proof (`ownLevelNotes(value.annotations).includes(note)`,
 * {@link findNoteOwner}) is reference identity, which can never match a note
 * object from a wholly separate compile.
 *
 * What DOES survive across two independent compiles of the same file is
 * TEXT POSITION: Malloy's parse is deterministic, so the same source
 * produces a note at the exact same `url`+`range` every time. Scans the
 * sibling's own top-level structs for whichever one's OWN-LEVEL notes carry
 * one at that same position — the sibling compile's equivalent of "this
 * struct is who wrote it".
 */
function findSiblingAnnotationDeclaringSource(
   note: NoteLike | undefined,
   resolveSibling: SiblingModelDefResolver | undefined,
): SourceDef | undefined {
   if (!note?.at || !resolveSibling) return undefined;
   const siblingModelDef = resolveSibling(note.at.url);
   if (!siblingModelDef) return undefined;
   for (const value of Object.values(siblingModelDef.contents)) {
      if (!isSourceDef(value)) continue;
      for (const candidate of ownLevelNotes(value.annotations)) {
         if (
            candidate.at &&
            candidate.at.url === note.at.url &&
            rangesEqual(candidate.at.range, note.at.range)
         ) {
            return value;
         }
      }
   }
   return undefined;
}

/** The first `.at` location carried by `condition`'s own field-usage list —
 *  where its earliest-listed field reference was originally parsed. Any one
 *  entry is enough: they all sit inside the same declaring `source:` block,
 *  since a condition is authored as a single expression in one place. */
function firstFieldUsageLocation(
   condition: FilterCondition,
): DocumentLocationLike | undefined {
   const refSummary = (condition as { refSummary?: RefSummaryLike }).refSummary;
   const first = refSummary?.fieldUsage?.[0] as
      | { at?: DocumentLocationLike }
      | undefined;
   return first?.at;
}

/**
 * The struct in `struct`'s own derivation chain that owns `condition` — the
 * farthest ancestor whose OWN `filterList` still contains `condition` BY
 * REFERENCE. Malloy never mutates a `FilterCondition` object once built — an
 * unmodified `extend` only ever spreads the array, never the elements
 * (`refined-source.js`) — so reference identity survives the copy at every
 * step, making this the same "not by name, not by sourceID" discriminator
 * `resolveGraftTarget` already uses for the analogous graft-target question.
 *
 * Walks THREE links, nearest first: {@link resolveDeclaredSource}'s
 * `sourceRegistry` walk (real for a plain join or an unmodified `source: a is
 * b`), then {@link findSourceByOwnAnnotationIdentity} (real for a MODIFIED
 * derivation that carries an authorize-tagged annotation, since Malloy
 * copies that note object onto the deriving struct BY REFERENCE regardless
 * of modification — see `gate_registry_walk.ts`'s module doc), then
 * {@link findConditionOriginByLocation} (the one link that survives a
 * MODIFIED derivation with NO annotation at all — 0.0.432 clears
 * `referenceID` on any modification and there is no note to identity-match,
 * but the condition's OWN field references still carry the location they
 * were parsed at, which is enough).
 *
 * Returns `struct` itself when none of the three links resolve AND
 * `resolveSibling` (see {@link findSiblingDeclaringSource}) also has nothing
 * — the caller ({@link assertInheritedSourceFiltersBind}) does NOT treat
 * that as "nothing to check": only a location-proven fresh filter (checked
 * separately, before this is even called) means that. An unresolved origin
 * here means "cannot prove where this came from", and the caller denies on
 * it.
 */
function findFilterOrigin(
   struct: SourceDef,
   condition: FilterCondition,
   modelDef: ModelDef | undefined,
   resolveSibling?: SiblingModelDefResolver,
): SourceDef {
   let origin = struct;
   let current = struct;
   const seen = new Set<SourceDef>([struct]);
   for (let depth = 0; depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      let next: SourceDef | undefined = modelDef
         ? nextDerivationLink(current, modelDef, seen)
         : undefined;
      if (!next && modelDef) {
         const at = firstFieldUsageLocation(condition);
         if (at) {
            const candidate = findConditionOriginByLocation(modelDef, at);
            if (candidate && !seen.has(candidate)) next = candidate;
         }
      }
      if (!next || !next.filterList?.includes(condition)) break;
      origin = next;
      seen.add(next);
      current = next;
   }
   if (origin === struct) {
      const sibling = findSiblingDeclaringSource(
         firstFieldUsageLocation(condition),
         resolveSibling,
      );
      if (sibling && siblingCandidateOwnsCondition(sibling, condition)) {
         return sibling;
      }
   }
   return origin;
}

/**
 * Whether `candidate` (found in a SIBLING compile, so never reference-equal
 * to anything in `condition`'s own compile) actually WROTE `condition` — same
 * `code` text and the same field-usage location — rather than merely
 * CONTAINING the position `findSiblingDeclaringSource` matched on.
 * Containment alone is not ownership: `findConditionOriginByLocation` picks
 * the narrowest struct whose own SPAN wraps the condition's location, but an
 * outer struct's span can wrap an inline join member's `extend { where: … }`
 * without that filter being on the outer struct's own `filterList` at all
 * (`child extend { join_one: j is … extend { where: … } on … }`: `child`'s
 * span contains `j`'s `where:` text, but the condition belongs to `j`, not
 * `child`). The served-model scan above already guards this with
 * `next.filterList?.includes(condition)`; a sibling compile has its own,
 * non-reference-equal `FilterCondition` objects, so this is that same check
 * translated across compiles.
 */
function siblingCandidateOwnsCondition(
   candidate: SourceDef,
   condition: FilterCondition,
): boolean {
   const at = firstFieldUsageLocation(condition);
   if (!at) return false;
   return !!candidate.filterList?.some((c) => {
      if (c.code !== condition.code) return false;
      const candidateAt = firstFieldUsageLocation(c);
      return (
         !!candidateAt &&
         candidateAt.url === at.url &&
         rangesEqual(candidateAt.range, at.range)
      );
   });
}

/** Whether `condition` reads any field at all — a `where: true`/`where:
 *  false` (or any filter whose expression names no field) has nothing that
 *  could have moved, so it is safe unconditionally, regardless of whether
 *  its origin resolves. Checked on the TOP-LEVEL `fieldUsage` list only
 *  (never the transitive closure {@link fieldUsageClosure} computes) — an
 *  empty top level means an empty closure too, since the closure only ever
 *  ADDS entries reachable FROM a non-empty seed. */
function conditionReadsNoField(condition: FilterCondition): boolean {
   const refSummary = (condition as { refSummary?: RefSummaryLike }).refSummary;
   return !refSummary?.fieldUsage || refSummary.fieldUsage.length === 0;
}

/**
 * Two structural, POSITIVE signals a condition is genuinely fresh (never a
 * negative inference from "unrecognized" — see {@link isOwnFreshFilter}).
 * `queryLocation` (`prepared._query.location`, set for every run) covers a
 * NAMED query's anonymous inline extend, which does not always get its own
 * `.location` (can carry its base's forward unchanged). `compiledUrl` is the
 * synthetic `internal://` URL a caller/cell compile gets; `undefined` for a
 * NAMED run, which compiles no such text of its own. `queryLocation` itself
 * is the plain MODEL FILE's span for a named run (`run: q1` / `queryName`),
 * and an `internal://` span for everything else — the same URL distinction
 * `compiledUrl` makes, just carried on the field that is never `undefined`.
 */
export interface FreshnessContext {
   compiledUrl?: string;
   queryLocation?: DocumentLocationLike;
}

/**
 * Whether `condition` is `struct`'s OWN, freshly-authored filter — never
 * merely because {@link findFilterOrigin} failed to resolve anything else.
 * "Unrecognized by `modelDef.contents`" is NOT proof: an inherited condition
 * from a narrower per-cell or selective-import compile is equally
 * unrecognized without being fresh. Proven instead by parse-location
 * containment in `struct`'s own span, or in `queryLocation`, or by
 * `at.url === compiledUrl`.
 */
function isOwnFreshFilter(
   struct: SourceDef,
   condition: FilterCondition,
   freshness: FreshnessContext,
): boolean {
   const at = firstFieldUsageLocation(condition);
   if (!at) return false;
   const structLocation = (struct as { location?: DocumentLocationLike })
      .location;
   if (structLocation && locationContains(structLocation, at)) return true;
   if (
      freshness.queryLocation &&
      locationContains(freshness.queryLocation, at)
   ) {
      return true;
   }
   return !!freshness.compiledUrl && at.url === freshness.compiledUrl;
}

/**
 * Assert every INHERITED entry in `struct.filterList` — an author `where:`
 * carried in from a base, or a grafted `#(access_filter)` condition — still
 * binds to the same fields it did where it was declared, then recurse into
 * every joined source reachable from `struct`. A condition that reads no
 * field or is genuinely `struct`'s OWN fresh filter is skipped; every other
 * condition must resolve to a declaring source and prove binding — an
 * unresolvable origin DENIES, never "must be the struct's own". That default
 * is the gap a caller-modified struct (not itself a `modelDef.contents`
 * entry) can otherwise exploit: "can't tell" and "fine" must not read the
 * same here. Runs for every executed struct regardless of whether an
 * `#(authorize)`/`#(access_filter)` annotation was ever involved — a plain
 * filtered source is just as misbindable and has no graft step to hook.
 *
 * `visited` bounds a genuine CYCLE with a silent return (already checked on
 * this walk). `depth` bounds how deep a walk may go before giving up on ever
 * reaching one; exhausting it THROWS rather than returning, because unlike a
 * cycle it proves nothing — a struct beyond the bound was never checked.
 */
export function assertInheritedSourceFiltersBind(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
   freshness: FreshnessContext = {},
   resolveSibling: SiblingModelDefResolver | undefined = undefined,
   depth = 0,
   visited: Set<SourceDef> = new Set(),
   alreadyProven: ReadonlySet<FilterCondition> = new Set(),
   // A member's own `where:` has no derivation link back to the member.
   compositeRunTarget: SourceDef | undefined = undefined,
): void {
   if (visited.has(struct)) return;
   if (depth > MAX_JOIN_RECURSION_DEPTH) {
      throw new Error(
         "row-security filter recursion exceeded the join-depth resolution bound",
      );
   }
   visited.add(struct);
   for (const condition of struct.filterList ?? []) {
      // A grafted condition's `.at` is where the graft compiled it, never
      // inside `struct`'s own span even when the gate genuinely IS
      // `struct`'s own — so this walk's location-based classification can't
      // read it, and shouldn't: the caller already proved-or-denied it by
      // annotation-note identity (`assertGraftedGateBindsToDeclaringSource`).
      if (alreadyProven.has(condition)) continue;
      if (conditionReadsNoField(condition)) continue;
      // Runs BEFORE the fresh-filter check: an unnamed inline `extend {}`
      // does not always get its own `.location` (it can carry its BASE's
      // forward unchanged), which would make that check wrongly "prove" a
      // condition the struct never wrote. `findFilterOrigin` only returns
      // something other than `struct` when an ancestor genuinely carries
      // this exact condition object, so it is the safe one to trust first.
      const origin = findFilterOrigin(
         struct,
         condition,
         modelDef,
         resolveSibling,
      );
      if (origin !== struct) {
         assertFilterConditionBindsToDeclaringSource(origin, struct, condition);
         continue;
      }
      if (isOwnFreshFilter(struct, condition, freshness)) continue;
      const member = compositeRunTarget
         ? memberDeclaringCondition(compositeRunTarget, struct, condition)
         : undefined;
      if (member) {
         assertFilterConditionBindsToDeclaringSource(member, struct, condition);
         continue;
      }
      throw new Error(
         "a row-security filter's declaring source could not be resolved",
      );
   }
   // `struct.fields` absent means `struct` isn't a well-formed compiled
   // `SourceDef` at all (a test double, or a caller that resolved something
   // other than a real struct) — nothing further to walk, and the
   // AUTHORITATIVE own-source gate above already covers the denial case for
   // an unresolvable target, so this skips rather than crashing.
   for (const field of struct.fields ?? []) {
      if (isJoined(field) && isSourceDef(field)) {
         assertInheritedSourceFiltersBind(
            field as unknown as SourceDef,
            modelDef,
            freshness,
            resolveSibling,
            depth + 1,
            visited,
            alreadyProven,
         );
      }
   }
}

/** Exported for `./filter.ts`'s `#(filter)` injection check and tests — see
 *  the module doc. */
export { fieldPathIdentical };
