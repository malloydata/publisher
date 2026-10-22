// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Guards against a filter resolving to the WRONG field once a caller frees
 * up the name it was written against (`rename:`, or `except:`/`accept:` then
 * reusing the name). Malloy resolves a filter's field references by name,
 * late, against whatever struct it ends up on, and never re-checks them. See
 * `docs/authorize.md`.
 *
 * Covers grafted `#(access_filter)`/`#(authorize)` conditions
 * ({@link assertGraftedGateBindsToDeclaringSource}), inherited `where:` and
 * joined sources' filters ({@link assertInheritedSourceFiltersBind}), and
 * `#(filter)` injection (`Model.assertFilterAnnotationsBindToDeclaringSource`).
 * All reuse {@link assertFilterConditionBindsToDeclaringSource}.
 *
 * "Identical field" means same `name`, `type`, and deep-equal `e`, ignoring
 * `annotations`, `accessModifier`, and `location`/`at`/`referencedAt` when the
 * value is a real `DocumentLocation` (all are also legal author names), and a
 * query source's per-compile `QuerySource-<uuid>` name. A join hop also
 * requires the same `join` and `matrixOperation` and deep-equal `onExpression`,
 * `filterList`, `parameters`, `arguments`, a query source's `query` and a
 * composite's `sources`, since same-table sources share a `name` and differ in
 * rows only through those. A joined composite that Malloy resolved to one
 * member is accepted only when the executed struct's own pre-resolution join
 * is that same composite ({@link isMalloyResolvedMember}); a member the caller
 * joined directly is refused. That acceptance is for inherited `where:` only: a
 * row-level gate or `#(filter)` dimension read through a joined composite stays
 * refused, because the member Malloy resolves depends on the caller's query. Locations from
 * different compiles are compared by canonical URL ({@link canonicalUrl}).
 * Fields are looked up by ACTIVE name ({@link activeName}).
 *
 * **The default is DENY.** Failing to resolve a declaring source or a field,
 * or hitting a walk bound, means "cannot prove this binds correctly". A
 * condition skips the check only when PROVEN to read no field
 * ({@link conditionReadsNoField}) or PROVEN to be the executed struct's own
 * fresh filter ({@link isOwnFreshFilter}).
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
import { fileURLToPath, pathToFileURL } from "url";
import { findSourceByOwnAnnotationIdentity } from "./gate_classification";
import { parseAuthorizeAnnotation } from "./authorize";
import { sameNotePosition } from "./annotations";

/**
 * The next struct back in a derivation chain, trying the same links the gate
 * walks use, nearest first: the `sourceRegistry` link
 * ({@link resolveDeclaredSource}), shared annotation notes
 * ({@link findSourceByOwnAnnotationIdentity}), then `query.structRef`
 * ({@link resolveQuerySourceBase}), which a `query_source`
 * (`Z is X -> {...}`) needs because it has no registry entry of its own.
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

/** Duck-typed: Malloy does not re-export `RefSummary` from its root. */
interface FieldUsageEntryLike {
   path: readonly string[];
}
interface RefSummaryLike {
   fieldUsage?: readonly FieldUsageEntryLike[];
}

/** Walk bounds: large enough for any real model, small enough that a
 *  pathological or cyclic one fails closed instead of hanging. */
const MAX_CLOSURE_SIZE = 256;
export const MAX_JOIN_RECURSION_DEPTH = 16;

/** A field's visible name (`as`, else `name`), as Malloy's unexported
 *  `activeName`. `fieldUsage` paths use visible names, so matching `.name`
 *  alone would falsely deny every aliased join. */
function activeName(f: { name: string; as?: string }): string {
   return f.as ?? f.name;
}

/** Stripped everywhere by NAME: neither collides with a name-keyed record
 *  (`parameters`/`arguments`/a record-literal's `kids`) anywhere in the IR. */
const IGNORED_KEYS_BY_NAME = new Set(["annotations", "accessModifier"]);

/** Stripped only when the VALUE is a `DocumentLocation`: these are also legal
 *  author parameter/record-field names, and stripping those by key would make
 *  two different bindings compare equal. */
const SHAPE_CHECKED_KEYS = new Set(["location", "at", "referencedAt"]);

/** Records keyed by the author's own names, whose KEYS {@link strip} must keep. */
const NAME_KEYED_RECORD_KEYS = new Set([
   "parameters",
   "arguments",
   "sourceArguments",
]);

/** Malloy mints a fresh name for each compile of a query source. */
const MINTED_QUERY_SOURCE_NAME = /^QuerySource-[0-9a-f-]{36}$/;

function hasMintedQuerySourceName(v: unknown): boolean {
   const o = v as { type?: unknown; name?: unknown } | undefined;
   return (
      o?.type === "query_source" &&
      typeof o.name === "string" &&
      MINTED_QUERY_SOURCE_NAME.test(o.name)
   );
}

/** Structural match for `DocumentLocation`; no Expr or Parameter matches it. */
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

// Keyed on IR objects, which are never mutated in place (every graft clones first); an in-place edit would leave stale JSON.
const strippedJsonCache = new WeakMap<object, string>();

/** Structural equality ignoring metadata keys. Name-keyed records are compared
 *  in sorted key order; any other key-order mismatch reads as "different",
 *  which fails closed, so it is not chased. */
function deepEqualIgnoring(a: unknown, b: unknown): boolean {
   return strippedJson(a) === strippedJson(b);
}

function strippedJson(value: unknown): string {
   if (!value || typeof value !== "object") {
      return JSON.stringify(strip(value));
   }
   let json = strippedJsonCache.get(value);
   if (json === undefined) {
      json = JSON.stringify(strip(value));
      strippedJsonCache.set(value, json);
   }
   return json;
}

function strip(value: unknown): unknown {
   if (Array.isArray(value)) return value.map(strip);
   if (value && typeof value === "object") {
      const obj = value as Record<string, unknown>;
      const mintedName = hasMintedQuerySourceName(obj);
      const out: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(obj)) {
         if (
            NAME_KEYED_RECORD_KEYS.has(k) ||
            (k === "kids" && obj.node === "recordLiteral")
         ) {
            const record = (v && typeof v === "object" ? v : {}) as Record<
               string,
               unknown
            >;
            // Sorted: argument order is not meaning (`f(a is 1, b is 2)`).
            out[k] = Object.fromEntries(
               Object.entries(record)
                  .sort(([x], [y]) => (x < y ? -1 : x > y ? 1 : 0))
                  .map(([rk, rv]) => [rk, strip(rv)]),
            );
            continue;
         }
         if (IGNORED_KEYS_BY_NAME.has(k)) continue;
         if (k === "name" && mintedName) continue;
         if (SHAPE_CHECKED_KEYS.has(k) && isDocumentLocation(v)) continue;
         out[k] = strip(v);
      }
      return out;
   }
   return value;
}

type JoinedLike = Record<string, unknown>;

/** Whether `executed` is the member Malloy picked for the declared joined
 *  `composite`, not one the caller joined: the executed struct's own
 *  pre-resolution join (`unresolved`) is still that same composite. */
function isMalloyResolvedMember(
   composite: FieldDef | undefined,
   executed: FieldDef | undefined,
   unresolved: FieldDef | undefined,
): boolean {
   if (!composite || !executed || !unresolved) return false;
   const c = composite as unknown as JoinedLike;
   const u = unresolved as unknown as JoinedLike;
   return (
      isJoined(composite) &&
      isJoined(unresolved) &&
      isComposite(composite as SourceDef) &&
      !isComposite(executed as SourceDef) &&
      c.join === u.join &&
      // Malloy drops a resolved composite join's type, so it can only be read here.
      c.matrixOperation === u.matrixOperation &&
      // `sources` is undefined on a non-composite `unresolved`, so this also requires it to be a composite.
      deepEqualIgnoring(c.sources, u.sources)
   );
}

/** Whether `a` and `b` are the same field (see the module doc). An
 *  unresolved side is never identical: the field genuinely moved. */
function fieldsIdentical(
   a: FieldDef | undefined,
   b: FieldDef | undefined,
   resolvedMember = false,
): boolean {
   if (!a || !b) return false;
   const ja = a as unknown as JoinedLike;
   const jb = b as unknown as JoinedLike;
   const joined = isJoined(a) || isJoined(b);
   if (joined && (!isJoined(a) || !isJoined(b))) return false;
   if (!resolvedMember) {
      if (a.type !== b.type) return false;
      if (
         a.name !== b.name &&
         !(hasMintedQuerySourceName(a) && hasMintedQuerySourceName(b))
      ) {
         return false;
      }
   }
   if (!deepEqualIgnoring((a as { e?: unknown }).e, (b as { e?: unknown }).e))
      return false;
   if (joined) {
      if (ja.join !== jb.join) return false;
      // A resolved member has lost its join type; it was checked against the pre-resolution join.
      if (!resolvedMember && ja.matrixOperation !== jb.matrixOperation) {
         return false;
      }
      if (!deepEqualIgnoring(ja.onExpression, jb.onExpression)) return false;
      // A same-table sibling (`where:`, or different parameter bindings) keeps the joined struct's `name`.
      const rows = (j: JoinedLike) => ({
         filterList: j.filterList ?? [],
         parameters: j.parameters ?? {},
         arguments: j.arguments ?? {},
         sourceArguments: j.sourceArguments ?? {},
      });
      if (!deepEqualIgnoring(rows(ja), rows(jb))) return false;
      // A query source's name is minted per compile, so its `query` is its identity.
      if (a.type === "query_source" && !deepEqualIgnoring(ja.query, jb.query)) {
         return false;
      }
      if (
         isComposite(a as SourceDef) &&
         isComposite(b as SourceDef) &&
         !deepEqualIgnoring(ja.sources, jb.sources)
      ) {
         return false;
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
 * Whether `path` resolves to the same field in `declaring` as in `executed`.
 * Every hop is checked, not just the leaf: a join swapped partway down the
 * path is a divergence even when the leaf matches. `executedUnresolved` is
 * `executed` before Malloy resolved its joined composites to members.
 */
function fieldPathIdentical(
   declaring: SourceDef,
   executed: SourceDef,
   path: readonly string[],
   executedUnresolved?: SourceDef,
): boolean {
   let dCur: SourceDef | undefined = declaring;
   let eCur: SourceDef | undefined = executed;
   let uCur: SourceDef | undefined = executedUnresolved;
   for (let i = 0; i < path.length; i++) {
      const name = path[i];
      const dField = dCur?.fields?.find((f) => activeName(f) === name);
      const eField = eCur?.fields?.find((f) => activeName(f) === name);
      const uField = uCur?.fields?.find((f) => activeName(f) === name);
      const resolvedMember = isMalloyResolvedMember(dField, eField, uField);
      if (!fieldsIdentical(dField, eField, resolvedMember)) return false;
      if (i < path.length - 1) {
         if (!dField || !isJoined(dField) || !eField || !isJoined(eField)) {
            return false;
         }
         const declared = dField as unknown as SourceDef;
         const executedJoin = eField as unknown as SourceDef;
         uCur =
            uField && isJoined(uField)
               ? (uField as unknown as SourceDef)
               : undefined;
         dCur = resolvedMember
            ? withCompositeMembersResolved(declared, executedJoin, uCur)
            : declared;
         eCur = executedJoin;
      }
   }
   return true;
}

/**
 * Every field-usage path reachable from `refSummary`, resolved TRANSITIVELY
 * through `declaring`, following: a dimension's own expression (a filter on
 * `authorized` lists only `authorized`), each join's ON/`with`, and each
 * joined source's own `where:`. Returns `truncated` past
 * {@link MAX_CLOSURE_SIZE}, or `unrecordedJoin` for a join whose ON or
 * `where:` reads fields without a `refSummary`; the caller must deny on
 * either.
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
      // A nested usage path is relative to the struct `field` lives on (the
      // join prefix of `path`), not to `declaring`; re-qualify it so every
      // queued path is `declaring`-relative.
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

/** A composite's stand-in for a member's field: a `compositeField` expression,
 *  or, for a join, an empty composite carrying `onCompositeFieldUsage`. */
function isCompositePlaceholder(field: FieldDef): boolean {
   return (
      (field as { e?: { node?: unknown } }).e?.node === "compositeField" ||
      (isComposite(field as SourceDef) &&
         (field as { onCompositeFieldUsage?: unknown })
            .onCompositeFieldUsage !== undefined)
   );
}

function isComposite(struct: SourceDef | undefined): boolean {
   return (struct as { type?: string } | undefined)?.type === "composite";
}

function compositeMembers(
   composite: SourceDef,
   out: SourceDef[] = [],
   depth = 0,
): SourceDef[] {
   if (depth > MAX_JOIN_RECURSION_DEPTH) return out;
   const members = (composite as { sources?: readonly SourceDef[] }).sources;
   for (const member of members ?? []) {
      out.push(member);
      if (isComposite(member)) compositeMembers(member, out, depth + 1);
   }
   return out;
}

/** Identity tells a member's field from a redefinition. The objects come
 *  from `composite`, compiled with `executed`; only leaves identical to a
 *  declaring leaf count, so a caller's `compose(...)` adds none. */
function withCompositeMembersResolved(
   declaring: SourceDef,
   executed: SourceDef,
   composite: SourceDef | undefined,
): SourceDef {
   if (!composite || !isComposite(declaring) || !isComposite(composite)) {
      return declaring;
   }
   const leaf = (member: SourceDef) => !isComposite(member);
   const declared = new Set(
      compositeMembers(declaring)
         .filter(leaf)
         .map((member) => strippedJson(member)),
   );
   const memberFields = new Set(
      compositeMembers(composite)
         .filter((member) => leaf(member) && declared.has(strippedJson(member)))
         .flatMap((member) => member.fields ?? []),
   );
   let resolved = false;
   const fields = (declaring.fields ?? []).map((field) => {
      if (!isCompositePlaceholder(field)) return field;
      const executedField = executed.fields?.find(
         (f) => activeName(f) === activeName(field),
      );
      if (!executedField || !memberFields.has(executedField)) return field;
      resolved = true;
      return executedField;
   });
   return resolved ? { ...declaring, fields } : declaring;
}

function membersDeclaringCondition(
   composite: SourceDef,
   condition: FilterCondition,
): SourceDef[] {
   return compositeMembers(composite).filter((member) =>
      member.filterList?.includes(condition),
   );
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
   executedUnresolved: SourceDef | undefined = undefined,
): void {
   const declaringStruct = withCompositeMembersResolved(
      declaringStructAsWritten,
      executedStruct,
      executedUnresolved,
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
      if (
         !fieldPathIdentical(
            declaringStruct,
            executedStruct,
            path,
            executedUnresolved,
         )
      ) {
         throw new Error(
            `a row-security filter references \`${path.join(".")}\`, which resolves to a different field on the executed source`,
         );
      }
   }
}

/**
 * The `#(filter)` analogue of {@link assertFilterConditionBindsToDeclaringSource}
 * for a bare dimension NAME. The transitive closure matters: for
 * `dimension: safe_col is val`, renaming `val` away leaves `safe_col`'s own
 * `.e` unchanged; only following into `val` catches the rebind.
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

/** Whether `a` sits strictly before `b` in a document. */
function isEarlierPosition(
   a: { line: number; character: number },
   b: { line: number; character: number },
): boolean {
   return a.line < b.line || (a.line === b.line && a.character < b.character);
}

/** Duck-typed `Note`/`AnnotationsDef`. */
interface NoteLike {
   text: string;
   at?: DocumentLocationLike;
}
interface AnnotationsDefLike {
   blockNotes?: readonly NoteLike[];
   notes?: readonly NoteLike[];
   inherits?: AnnotationsDefLike;
}

/** Every note at EVERY level of the `inherits` chain, own-level first.
 *  Malloy demotes inherited notes to `inherits` once a struct declares any
 *  annotation of its own, so top-level notes alone can miss the note that
 *  produced an inherited filter. Bounded so malformed IR cannot loop. */
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

/** A struct's OWN-level notes only: widening to `inherits` would make every
 *  inheriting derivation look like the note's author. */
function ownLevelNotes(
   annotations: AnnotationsDefLike | undefined,
): NoteLike[] {
   return [...(annotations?.blockNotes ?? []), ...(annotations?.notes ?? [])];
}

/**
 * The top-level struct that ACTUALLY WROTE one of `notes`, by LOCATION (as
 * `source_extraction.ts`'s `considerNoteOwner`). Identity cannot tell the
 * declarer from an inheritor sharing the note by reference.
 *
 * Each note is resolved independently: positions are only comparable among
 * candidates for the SAME note, which share a file; two notes may resolve in
 * different files.
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
 * Whether `note` is an `#(access_filter)`/`#(authorize)` gate annotation, the
 * only kind the owner search may consider: a derived source's own render tag
 * or `#(doc)` would otherwise resolve as the "owner" and make a cross-file
 * misbind look self-contained. A malformed annotation is just not a gate note
 * here; load-time validation refuses it elsewhere.
 */
function isGateNote(note: NoteLike): boolean {
   try {
      return parseAuthorizeAnnotation(note.text) !== null;
   } catch {
      return false;
   }
}

/**
 * The struct that declared `struct`'s gate: {@link findNoteOwner} over its
 * gate notes at every level, then the structural walk
 * ({@link nextDerivationLink}; a `query_source` has no `annotations` at all),
 * then a sibling compile ({@link findSiblingAnnotationDeclaringSource}), then the
 * composite member Malloy resolved.
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
   // A composite entry point (`comp is compose(a, b) -> {...}`) never carries
   // the gate note: Malloy resolves one concrete member, whose own
   // annotations entry-point gate collection reads.
   return findCompositeMemberDeclaringSource(struct, modelDef);
}

/**
 * The declaring struct for a gate reachable only through the composite member
 * Malloy resolved `struct` against (undefined for a non-composite entry
 * point). Runs the same own-notes-then-structural-walk search from the
 * member, which may itself be a further derivation.
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
 * The `#(filter)` analogue of {@link findAnnotationDeclaringSource}, narrowed
 * to the note that produced `filter`. Needed because `filterMap` keys every
 * inheriting derivation too, and comparing a derived source to itself would
 * always look identical. Matches notes by PARSED definition, since the caller
 * holds only the `FilterDefinition`.
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
 * Assert that a GRAFTED `#(access_filter)`/`#(authorize)` condition binds to
 * the same fields at `executedStruct` as at the struct whose annotation wrote
 * it. A grafted condition is compiled fresh, so there is no shared object to
 * walk back through; the declarer is found by {@link findAnnotationDeclaringSource}.
 *
 * Always compares, even with no visible derivation: struct instances are
 * never reference-equal across compiles, and a shortcut loose enough to fire
 * there would also skip the misbindable case. THROWS when the entry point or
 * its declarer cannot be resolved: an unprovable gate denies.
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

/** Duck-typed `DocumentLocation`. */
export interface DocumentLocationLike {
   url: string;
   range: {
      start: { line: number; character: number };
      end: { line: number; character: number };
   };
}

/** Malloy records an imported file's URL as written (`a[1].malloy`) but the package's own compile percent-encodes it by `pathToFileURL`'s rules, so cross-compile comparisons need that one spelling. */
function canonicalUrl(url: string): string {
   try {
      return url.startsWith("file:")
         ? pathToFileURL(fileURLToPath(url)).href
         : new URL(url).href;
   } catch {
      return url;
   }
}

function samePosition(a: NoteLike["at"] & {}, b: NoteLike["at"] & {}): boolean {
   return sameNotePosition(
      { ...a, url: canonicalUrl(a.url) },
      { ...b, url: canonicalUrl(b.url) },
   );
}

function sameLocation(
   a: DocumentLocationLike | undefined,
   b: DocumentLocationLike | undefined,
): boolean {
   return (
      !!a &&
      !!b &&
      canonicalUrl(a.url) === canonicalUrl(b.url) &&
      JSON.stringify(a.range) === JSON.stringify(b.range)
   );
}

/** Whether `outer` fully contains `inner`, in the same file. */
function locationContains(
   outer: DocumentLocationLike,
   inner: DocumentLocationLike,
): boolean {
   if (canonicalUrl(outer.url) !== canonicalUrl(inner.url)) return false;
   if (isEarlierPosition(inner.range.start, outer.range.start)) return false;
   if (isEarlierPosition(outer.range.end, inner.range.end)) return false;
   return true;
}

/** A rough same-file span size where lines dominate characters: enough to
 *  pick the narrowest containing struct. */
function spanSize(range: DocumentLocationLike["range"]): number {
   return (
      (range.end.line - range.start.line) * 100_000 +
      (range.end.character - range.start.character)
   );
}

/**
 * The narrowest struct whose own span CONTAINS `at`. The last resort for an
 * unannotated `where:` inherited through a MODIFIED derivation, which keeps
 * no registry link and no note to match; but the condition's field
 * references keep the location they were parsed at, inside the `source:`
 * block that wrote them. The caller still requires the candidate's
 * `filterList` to contain the condition, so position alone never decides.
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
 * Resolves the `ModelDef` the package compiled for a `file://` URL (every
 * served file is its own `Model`). Covers ancestors a selective import or a
 * per-cell compile left out of `modelDef.contents`. Wired by
 * `Package.applySiblingModelResolverToModels`; a Model outside a Package has
 * none, so resolution misses and the check denies.
 */
export type SiblingModelDefResolver = (url: string) => ModelDef | undefined;

/**
 * The declaring struct for a condition's location `at`, found by span
 * containment in the sibling compile. The binding check that follows
 * compares fields structurally, so a wrong candidate just fails to bind.
 */
function findSiblingDeclaringSource(
   at: DocumentLocationLike | undefined,
   resolveSibling: SiblingModelDefResolver | undefined,
): SourceDef | undefined {
   if (!at || !resolveSibling) return undefined;
   const siblingModelDef = resolveSibling(canonicalUrl(at.url));
   if (!siblingModelDef) return undefined;
   return findConditionOriginByLocation(siblingModelDef, at);
}

/**
 * The declaring struct for an ANNOTATION note in the sibling compile. A note
 * sits before its `source:` (so span containment does not apply) and object
 * identity never matches across compiles, but the parse is deterministic, so
 * the struct whose own-level notes include one at the same position wrote it.
 */
function findSiblingAnnotationDeclaringSource(
   note: NoteLike | undefined,
   resolveSibling: SiblingModelDefResolver | undefined,
): SourceDef | undefined {
   if (!note?.at || !resolveSibling) return undefined;
   const siblingModelDef = resolveSibling(canonicalUrl(note.at.url));
   if (!siblingModelDef) return undefined;
   for (const value of Object.values(siblingModelDef.contents)) {
      if (!isSourceDef(value)) continue;
      for (const candidate of ownLevelNotes(value.annotations)) {
         if (candidate.at && samePosition(candidate.at, note.at)) {
            return value;
         }
      }
   }
   return undefined;
}

/** Where `condition`'s first field reference was parsed. Any one entry is
 *  enough: a condition is authored as one expression in one place. */
function firstFieldUsageLocation(
   condition: FilterCondition,
): DocumentLocationLike | undefined {
   const refSummary = (condition as { refSummary?: RefSummaryLike }).refSummary;
   const first = refSummary?.fieldUsage?.[0] as
      | { at?: DocumentLocationLike }
      | undefined;
   return first?.at;
}

/** Same declaration, not merely the same object: a registry-reference copy
 *  shares its original's `sourceID`, location, `filterList` and `fields`
 *  elements, while any modification replaces or adds at least one of them. */
export function isSameDeclaration(a: SourceDef, b: SourceDef): boolean {
   if (a === b) return true;
   const sameElements = (x?: readonly unknown[], y?: readonly unknown[]) =>
      (x?.length ?? 0) === (y?.length ?? 0) &&
      (x ?? []).every((e, i) => e === (y as readonly unknown[])[i]);
   const sourceId = (s: SourceDef) => (s as { sourceID?: string }).sourceID;
   return (
      sourceId(a) !== undefined &&
      sourceId(a) === sourceId(b) &&
      !!a.location &&
      !!b.location &&
      a.location.url === b.location.url &&
      JSON.stringify(a.location.range) === JSON.stringify(b.location.range) &&
      sameElements(a.filterList, b.filterList) &&
      sameElements(a.fields, b.fields)
   );
}

/**
 * The farthest ancestor of `struct` whose own `filterList` still contains
 * `condition` BY REFERENCE. Malloy never mutates a `FilterCondition` (an
 * `extend` spreads the array, not the elements), so identity survives each
 * copy. Links tried per step: {@link nextDerivationLink}, then
 * {@link findConditionOriginByLocation} for a modified, unannotated
 * derivation (Malloy clears `referenceID` on any modification).
 *
 * `struct`'s own twin ({@link isSameDeclaration}) and a non-composite
 * `unresolved` (`struct` before its joined composites were resolved) are walked
 * through but never returned: they carry the same joins as `struct`, so
 * comparing against them proves nothing.
 *
 * Returns `struct` itself when nothing resolves, including the sibling
 * compile; the caller treats that as "cannot prove", not "nothing to check".
 */
function findFilterOrigin(
   struct: SourceDef,
   condition: FilterCondition,
   modelDef: ModelDef | undefined,
   resolveSibling?: SiblingModelDefResolver,
   unresolved?: SourceDef,
   virtualUrl?: string,
): SourceDef {
   let origin = struct;
   let current = struct;
   let twin: SourceDef | undefined;
   let ancestorUnresolved = false;
   const seen = new Set<SourceDef>([struct]);
   for (let depth = 0; depth < ANCESTOR_WALK_MAX_DEPTH; depth++) {
      let next: SourceDef | undefined = modelDef
         ? nextDerivationLink(current, modelDef, seen)
         : undefined;
      // A derivation link can name the wrong ancestor (a query source's base),
      // so the span of the condition's own text is tried whenever it fails.
      if (modelDef && !next?.filterList?.includes(condition)) {
         const at = firstFieldUsageLocation(condition);
         const candidate = at && findConditionOriginByLocation(modelDef, at);
         if (candidate && !seen.has(candidate)) {
            next ??= candidate;
            if (candidate.filterList?.includes(condition)) next = candidate;
         }
      }
      if (!next || !next.filterList?.includes(condition)) {
         ancestorUnresolved = !next;
         break;
      }
      if (
         !isSameDeclaration(next, struct) &&
         (!unresolved ||
            !isSameDeclaration(next, unresolved) ||
            isComposite(unresolved))
      ) {
         origin = next;
      } else {
         twin ??= next;
      }
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
      // An inline `X extend { where: }` carries X's location, so X (which does not hold the new where:) is the struct whose fields the where: was written against.
      const inlineBase = Object.values(modelDef?.contents ?? {}).find(
         (v) =>
            isSourceDef(v) &&
            v !== struct &&
            !isSameDeclaration(v, struct) &&
            !v.filterList?.includes(condition) &&
            sameLocation(v.location, struct.location),
      );
      if (inlineBase) return inlineBase as SourceDef;
      // A twin is only a same-named struct, not the declaration, so it stands in only where no sibling compile can exist: a model with no resolver, or the /compile text compiled for this request alone.
      const at = firstFieldUsageLocation(condition);
      const inVirtualFile =
         !!at &&
         !!virtualUrl &&
         canonicalUrl(at.url) === canonicalUrl(virtualUrl);
      if (twin && ancestorUnresolved && (!resolveSibling || inVirtualFile)) {
         return twin;
      }
   }
   return origin;
}

/**
 * Whether a sibling-compile `candidate` actually WROTE `condition` (same
 * `code` and field-usage position), not merely contains its position: an
 * outer struct's span can wrap an inline join member's `where:` that belongs
 * to the member. The cross-compile form of `filterList.includes(condition)`.
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
      return !!candidateAt && samePosition(candidateAt, at);
   });
}

/** Whether `condition` reads no field (e.g. `where: true`): nothing could
 *  have moved, so it is safe whether or not its origin resolves. An empty
 *  top-level `fieldUsage` implies an empty closure. */
function conditionReadsNoField(condition: FilterCondition): boolean {
   const refSummary = (condition as { refSummary?: RefSummaryLike }).refSummary;
   return !refSummary?.fieldUsage || refSummary.fieldUsage.length === 0;
}

/**
 * POSITIVE signals that a condition is fresh (see {@link isOwnFreshFilter}).
 * `queryLocation` (`prepared._query.location`, set for every run) covers a
 * named query's inline extend, which may carry its base's `.location`.
 * `compiledUrl` is the synthetic `internal://` URL of a caller or cell
 * compile; undefined for a named run. `virtualUrl` is the `/compile` check
 * file that re-translates the author's own model text for this one request, so
 * no sibling compile exists for what it holds.
 */
export interface FreshnessContext {
   compiledUrl?: string;
   queryLocation?: DocumentLocationLike;
   virtualUrl?: string;
}

/**
 * Whether `condition` is PROVEN to be `struct`'s own fresh filter, by its
 * parse location lying in `struct`'s span or `queryLocation`, or in
 * `compiledUrl`. Being unrecognized is not proof: an inherited condition
 * from a narrower compile is unrecognized too.
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
 * Assert every INHERITED entry in `struct.filterList` (an inherited `where:`
 * or a grafted condition) still binds as where it was declared, then recurse
 * into every joined source, a query source's base, and every join a query
 * stage (`pipeline`, or a query source's own) adds. Only a condition that
 * reads no field or is proven fresh is skipped; an unresolvable origin
 * DENIES, since a caller-modified struct could otherwise exploit "can't tell"
 * reading as "fine". Runs for every executed struct, gated or not: a plain
 * filtered source is just as misbindable.
 *
 * `visited` ends a cycle silently (already checked). Exhausting `depth`
 * THROWS: a struct beyond the bound was never checked.
 */
export function assertInheritedSourceFiltersBind(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
   freshness: FreshnessContext = {},
   resolveSibling: SiblingModelDefResolver | undefined = undefined,
   depth = 0,
   visited: Set<SourceDef> = new Set(),
   alreadyProven: ReadonlySet<FilterCondition> = new Set(),
   // `struct` before composite resolution, from the same compile.
   unresolved: SourceDef | undefined = undefined,
   // The run query's own stages, whose `extend:` joins hang off no struct.
   pipeline: readonly StageLike[] | undefined = undefined,
): void {
   if (visited.has(struct)) return;
   if (depth > MAX_JOIN_RECURSION_DEPTH) {
      throw new Error(
         "row-security filter recursion exceeded the join-depth resolution bound",
      );
   }
   visited.add(struct);
   const composite = isComposite(unresolved) ? unresolved : undefined;
   for (const condition of struct.filterList ?? []) {
      // A grafted condition's location is where the graft compiled it, so
      // this walk cannot classify it; the caller already proved it
      // (`assertGraftedGateBindsToDeclaringSource`).
      if (alreadyProven.has(condition)) continue;
      if (conditionReadsNoField(condition)) continue;
      // Before the fresh-filter check: an inline `extend {}` may carry its
      // base's `.location`, which would wrongly "prove" an inherited
      // condition fresh. A non-`struct` origin is always a genuine ancestor
      // holding this exact condition object.
      const origin = findFilterOrigin(
         struct,
         condition,
         modelDef,
         resolveSibling,
         unresolved,
         freshness.virtualUrl,
      );
      if (origin !== struct) {
         assertFilterConditionBindsToDeclaringSource(
            origin,
            struct,
            condition,
            unresolved,
         );
         continue;
      }
      if (isOwnFreshFilter(struct, condition, freshness)) continue;
      const members = composite
         ? membersDeclaringCondition(composite, condition)
         : [];
      if (members.length > 0) {
         for (const member of members) {
            assertFilterConditionBindsToDeclaringSource(
               member,
               struct,
               condition,
            );
         }
         continue;
      }
      throw new Error(
         "a row-security filter's declaring source could not be resolved",
      );
   }
   const descend = (next: SourceDef, nextUnresolved?: SourceDef) =>
      assertInheritedSourceFiltersBind(
         next,
         modelDef,
         freshness,
         resolveSibling,
         depth + 1,
         visited,
         alreadyProven,
         nextUnresolved,
      );
   // No `fields` means not a real compiled struct (e.g. a test double):
   // nothing to walk, and the filter check above already denies as needed.
   for (const field of struct.fields ?? []) {
      if (isJoined(field) && isSourceDef(field)) {
         const unresolvedJoin = unresolved?.fields?.find(
            (f) => isJoined(f) && activeName(f) === activeName(field),
         );
         descend(
            field as unknown as SourceDef,
            unresolvedJoin as unknown as SourceDef | undefined,
         );
      }
   }
   // A query source's rows come from its base and stages, none of which are in `fields`.
   if ((struct as { type?: string }).type === "query_source") {
      const base = resolveQuerySourceBase(struct, modelDef);
      if (!base) {
         throw new Error("a query source's base could not be resolved");
      }
      const resolvedBase = resolveCompositeResolvedBase(struct);
      descend(resolvedBase ?? base, resolvedBase ? base : undefined);
      const own = (struct as { query?: { pipeline?: readonly StageLike[] } })
         .query;
      stageJoins(own?.pipeline).forEach((join) => descend(join));
   }
   stageJoins(pipeline).forEach((join) => descend(join));
}

/** Duck-typed query stage: only the parts that can carry a join. */
export interface StageLike {
   extendSource?: readonly FieldDef[];
   queryFields?: readonly { type?: string; pipeline?: readonly StageLike[] }[];
}

/** Every source joined by `extend:` in `pipeline`, including inside nests; unlike `collectCallerJoins`' pipeline walk, regardless of who wrote it. */
function stageJoins(
   pipeline: readonly StageLike[] | undefined,
   depth = 0,
): SourceDef[] {
   if (depth > MAX_JOIN_RECURSION_DEPTH) {
      throw new Error(
         "row-security filter recursion exceeded the join-depth resolution bound",
      );
   }
   const joins: SourceDef[] = [];
   for (const stage of pipeline ?? []) {
      for (const field of stage.extendSource ?? []) {
         if (isJoined(field) && isSourceDef(field)) {
            joins.push(field as unknown as SourceDef);
         }
      }
      for (const field of stage.queryFields ?? []) {
         if (field.type === "turtle") {
            joins.push(...stageJoins(field.pipeline, depth + 1));
         }
      }
   }
   return joins;
}

/** Exported for tests. */
export { fieldPathIdentical };
