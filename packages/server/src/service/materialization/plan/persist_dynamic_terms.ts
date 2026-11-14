// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Annotations, type PersistSource } from "@malloydata/malloy";
import { parseRouteTag } from "../../motly";
import { collectGivenRefs } from "../../given";
import { readsAGiven } from "../../probe_compiler";
import { forEachFieldPath } from "../../governance/ir/array_query";
import {
   NOW_OFFSET_UNITS,
   type NowComparisonOp,
   type NowOffsetUnit,
} from "../../malloy_text";

/**
 * Where a persist source's givens sit, and whether that lets the source be
 * materialized into a storage destination and served per caller.
 *
 * The rule this module turns on (established by probing Malloy): a persist
 * source's build SQL is the persisted relation ALONE. Its own `filterList`
 * (extend-block `where:`) and `fields` (`dimension:` / `measure:` / `join:`)
 * are NOT in it; they refine the relation when it is READ. A given there is
 * never frozen into the artifact and is re-applied per caller at read.
 *
 * A given ANYWHERE ELSE is substituted at build time with the declaration
 * default, so every caller is served one caller's slice:
 * {@link buildSubstitutesAGiven}, a hard refusal on both tiers.
 *
 * Of the read-time positions only two are admitted: an extend-block `where:`,
 * and a join to a given-scoped source itself materialized into storage
 * ({@link JoinedDynamicTerms}). The others are not in the build either, but
 * the serve shape has nothing that re-binds the given there. Each gets its own
 * reason so the refusal names the position.
 */
type DynamicTermRefusal =
   | "given_in_persisted_query"
   | "dynamic_projection"
   | "dynamic_join"
   | "dynamic_joined_where";

/**
 * One extend-block `where:` conjunct that references a given: absent from the
 * build, re-applied at read. `code` is the verbatim text the serve shape
 * re-emits; `givens` are the names the shape must declare for it to compile.
 */
export interface DynamicTerm {
   code: string;
   givens: string[];
   /**
    * The source's OWN columns this term constrains (single-segment paths). An
    * empty list means the term cannot be expressed over the stored table's
    * columns. Read by the incremental apply to scope a `merge_key=` match; not
    * on the wire.
    */
   columns: string[];
}

/**
 * A join on the persisted source to a source itself scoped by a given, admitted
 * because per-caller filtering rides the JOINED source's artifact. The serve
 * shape re-emits the join only when `source` is bound, and that binding
 * re-applies `terms` with the caller's values; when it is not bound the joining
 * source is withheld and serves live. `alias` is dotted for a nested admitted
 * join (`grant.team`).
 */
interface JoinedDynamicTerms {
   alias: string;
   source: string;
   terms: DynamicTerm[];
}

type DynamicTermClassification =
   | { ok: true; terms: DynamicTerm[]; joinedTerms?: JoinedDynamicTerms[] }
   | { ok: false; reason: DynamicTermRefusal; detail: string };

type FieldRefusal = { ok: false; reason: DynamicTermRefusal; detail: string };

/** Max IR depth to walk; deep enough for real sources, a hang backstop. */
const MAX_WALK_DEPTH = 200;

/** The given names referenced anywhere in a slice of IR, in first-seen order. */
function givenNames(node: unknown): string[] {
   const names = new Set<string>();
   collectGivenRefs(node, names);
   return [...names];
}

/**
 * Whether the BUILD substitutes a given's value into the relation it writes.
 *
 * For a query-shaped source, the query's transitive `givenUsage` answers it
 * (verified against `getSQL()`: givens inside the persisted query or the input
 * source's `where:` report there; the persist source's own extend block does
 * not). A non-query source (`base extend { where: … }`) builds the base
 * unfiltered, so a structural walk over everything but the two read-time keys
 * answers instead. Fails closed: unreadable IR is a refusal.
 */
export function buildSubstitutesAGiven(persistSource: PersistSource): boolean {
   let def: unknown;
   try {
      def = persistSource._sourceDef as unknown;
   } catch {
      return true;
   }
   return defSubstitutesAGiven(def);
}

/** {@link buildSubstitutesAGiven} over a compiled source definition. */
function defSubstitutesAGiven(def: unknown): boolean {
   try {
      if (def === null || typeof def !== "object") {
         throw new Error("compiled source definition is not readable");
      }
      const query = (def as { query?: unknown }).query;
      if (query !== null && typeof query === "object") {
         const usage = (query as { givenUsage?: unknown }).givenUsage;
         // The marker summarises what the pipeline REFERENCES, and a source
         // ARGUMENT (`pp(x is $ORG_ID) -> { … }`) binds without being
         // referenced — so an empty marker is only trusted once the argument
         // holders are clear.
         if (Array.isArray(usage) && usage.length > 0) return true;
         if (argumentBindsAGiven(query)) return true;
         if (Array.isArray(usage)) return false;
      }
      const {
         filterList: _readTimeFilters,
         fields: _readTimeFields,
         ...relation
      } = def as Record<string, unknown>;
      return readsAGiven(relation, new WeakSet());
   } catch {
      return true;
   }
}

/**
 * Whether a given is bound into the relation as a source ARGUMENT
 * (`pp(x is $ORG_ID) -> { … }`). That bakes the default into the build SQL
 * but binds at `structRef` construction, so the `givenUsage` marker reads empty.
 *
 * Every `arguments` holder under the query is searched, not just the query's
 * own: a given binding a JOINED source on the input sits under
 * `structRef.fields` and the marker misses it too (measured), as does a chain
 * of parameterized sources. The cost, accepted: a parameterized given-filtered
 * join the query does NOT read is refused as well. Over-refusal loses a tier;
 * under-refusal loses tenant isolation.
 *
 * A CONSTANT argument bakes too but has no per-caller binding, so it is left
 * alone (the shape `parameter-eligibility` admits).
 */
function argumentBindsAGiven(node: unknown, depth = 0): boolean {
   if (depth > MAX_WALK_DEPTH) {
      throw new Error("argument walk exceeded max depth");
   }
   if (node === null || typeof node !== "object") return false;
   if (Array.isArray(node)) {
      return node.some((item) => argumentBindsAGiven(item, depth + 1));
   }
   for (const [key, value] of Object.entries(node)) {
      if (
         (key === "arguments" || key === "sourceArguments") &&
         readsAGiven(value, new WeakSet())
      ) {
         return true;
      }
      if (argumentBindsAGiven(value, depth + 1)) return true;
   }
   return false;
}

/**
 * Admit or refuse a persist source by the POSITION of every given it reads,
 * returning the extend-block `where:` terms that may be stripped from the
 * build and re-applied at read, or the first position that cannot be.
 *
 * The terms are the given-reading subset of {@link classifyWhereTerms}, the one
 * `where:` term classifier; this pass decides only where the givens may sit.
 * The baked check runs FIRST: a baked given is the one position no serve shape
 * can rescue, so reporting anything else would send the author to the wrong
 * line. Fails closed: a given in a position this pass does not know is
 * refused, since the failure mode is serving one tenant's rows to another.
 */
export function admitGivenPositions(
   persistSource: PersistSource,
): DynamicTermClassification {
   let def: unknown;
   try {
      def = persistSource._sourceDef as unknown;
   } catch (err) {
      return unreadable(err, "which givens the build would bake in");
   }
   return classifyDef(def, 0);
}

/**
 * {@link admitGivenPositions} over a compiled source definition. Separate so a
 * joined source can be held to the same rule as the source joining it: `depth`
 * counts the admitted joins between here and the persisted source.
 */
function classifyDef(def: unknown, depth: number): DynamicTermClassification {
   if (defSubstitutesAGiven(def)) {
      return {
         ok: false,
         reason: "given_in_persisted_query",
         detail:
            `a given is read while the persisted relation is BUILT, so its ` +
            `value is substituted at build time — from the declaration's ` +
            `default, the only value available then — and every caller is ` +
            `served that one slice. Move the given out of the persisted query ` +
            `and into the source's extend block (\`where: …\`), where it is ` +
            `left out of the build and applied per caller when the artifact ` +
            `is read`,
      };
   }
   if (def === null || typeof def !== "object") {
      return unreadable(
         new Error("compiled source definition is not readable"),
         "which givens the build would bake in",
      );
   }
   const record = def as Record<string, unknown>;

   try {
      const joinedTerms: JoinedDynamicTerms[] = [];
      const fieldRefusal = classifyFields(record.fields, joinedTerms, depth);
      if (fieldRefusal) return fieldRefusal;

      const terms = givenTerms(classifyWhereTerms(record));

      // Fail-closed sweep: `filterList` and `fields` were classified above and
      // `query` cleared by the baked check; a given anywhere else is refused.
      const {
         filterList: _accounted,
         fields: _alsoAccounted,
         query: _cleared,
         ...rest
      } = record;
      if (readsAGiven(rest, new WeakSet())) {
         return {
            ok: false,
            reason: "given_in_persisted_query",
            detail:
               `a given sits in a part of the source this pass cannot place ` +
               `— it is neither an extend-block \`where:\` nor a field ` +
               `declared on the source — so the publisher cannot prove the ` +
               `build leaves it out`,
         };
      }
      return joinedTerms.length > 0
         ? { ok: true, terms, joinedTerms }
         : { ok: true, terms };
   } catch (err) {
      return unreadable(err, "the build leaves every given out");
   }
}

function unreadable(err: unknown, cannotProve: string): FieldRefusal {
   return {
      ok: false,
      reason: "given_in_persisted_query",
      detail:
         `its given usage could not be determined ` +
         `(${err instanceof Error ? err.message : String(err)}), so the ` +
         `publisher cannot prove ${cannotProve}`,
   };
}

/**
 * The first field position carrying a given that is not admitted, or undefined.
 * Admitted given-scoped joins are appended to `joinedTerms`.
 *
 * No field is in the build SQL, so admission is decided by the SERVE shape. A
 * join to a given-scoped source is admitted: the shape re-emits it only when
 * its target is bound (`extractJoins`), and the target's binding re-applies
 * its own `where:`; otherwise queries through it serve live. See
 * {@link joinedSourceRefusal}.
 *
 * Refused: a given in a join's `on:` (`dynamic_join`), which nothing re-binds
 * per caller, and a dimension or measure reading a given itself
 * (`dynamic_projection`). A field reading a given only THROUGH an admitted join
 * carries none of its own (Malloy's summaries do not cross a join path).
 */
function classifyFields(
   fields: unknown,
   joinedTerms: JoinedDynamicTerms[],
   depth: number,
): FieldRefusal | undefined {
   if (!Array.isArray(fields)) return undefined;
   for (const field of fields) {
      if (field === null || typeof field !== "object") continue;
      const f = field as Record<string, unknown>;
      // `name` on a join is the joined source's identity (a `sql://…` URL);
      // `as` is the alias the author wrote, which is the one worth naming.
      const label =
         typeof f.as === "string"
            ? f.as
            : typeof f.name === "string"
              ? f.name
              : "(unnamed)";

      if (f.join !== undefined) {
         if (readsAGiven(f.onExpression, new WeakSet())) {
            return {
               ok: false,
               reason: "dynamic_join",
               detail:
                  `the join '${label}' has a given in its \`on:\` condition, ` +
                  `which decides which rows join per caller`,
            };
         }
         // Everything else under the join: its target's own `where:`, and its
         // target's fields.
         if (readsAGiven(f, new WeakSet())) {
            const joined = joinedSourceRefusal(f, label, depth);
            if (!joined.ok) return joined;
            joinedTerms.push(...joined.joinedTerms);
         }
         continue;
      }

      if (readsAGiven(f, new WeakSet())) {
         return {
            ok: false,
            reason: "dynamic_projection",
            detail:
               `the field '${label}' reads a given, so its value would ` +
               `differ per caller`,
         };
      }
   }
   return undefined;
}

/**
 * Keys a join field carries beyond the joined source's own definition, stripped
 * so the target is classified as it would be persisted (the `on:` is checked
 * separately, and `refSummary` covers the condition and target together).
 */
const JOIN_ONLY_KEYS = [
   "join",
   "onExpression",
   "matrixOperation",
   "refSummary",
   "referenceID",
] as const;

/**
 * Whether a join to a given-scoped source can be admitted, and the terms its
 * target re-applies when it can. Each failing condition is already fail-closed
 * at serve time (the query serves live); it is refused here to name the reason:
 *
 * 1. Joined BY NAME. A join target with any refinement of its own
 *    (`g extend { … }`) is compiled as an anonymous source with no `sourceID`,
 *    so nothing maps it to a binding and it can never be re-emitted.
 * 2. Declared `#@ persist … storage=`. Only a storage binding is on the serve
 *    shape, so a join to a source that is not materialized there is dropped.
 * 3. Admissible itself, by this same classification. That is what makes the
 *    rule transitive: a target that joins a further given-scoped source is held
 *    to the rule this source is, one level down.
 */
function joinedSourceRefusal(
   f: Record<string, unknown>,
   label: string,
   depth: number,
): FieldRefusal | { ok: true; joinedTerms: JoinedDynamicTerms[] } {
   const refuse = (why: string): FieldRefusal => ({
      ok: false,
      reason: "dynamic_joined_where",
      detail: `the source joined as '${label}' is itself scoped by a given, ${why}`,
   });

   if (depth >= MAX_JOIN_DEPTH) {
      return refuse(`and is joined more than ${MAX_JOIN_DEPTH} levels deep`);
   }
   const sourceID = f.sourceID;
   if (typeof sourceID !== "string" || sourceID.length === 0) {
      return refuse(
         `and is joined through a refinement (\`… extend { … }\`) rather than ` +
            `by name, so the serve shape cannot bind the join to that source's ` +
            `stored table. Declare the refinement on a named source and join ` +
            `that`,
      );
   }
   const source = sourceID.split("@")[0] || sourceID;
   if (!declaresStorage(f.annotations)) {
      return refuse(
         `and '${source}' is not materialized into a storage destination, so ` +
            `the join has no stored table to read per caller. Give '${source}' ` +
            `\`#@ persist … storage=\`, or drop \`storage=\` from this source`,
      );
   }

   const target: Record<string, unknown> = { ...f };
   for (const key of JOIN_ONLY_KEYS) delete target[key];
   const inner = classifyDef(target, depth + 1);
   if (!inner.ok) {
      return refuse(
         `and '${source}' could not itself be materialized per caller: ` +
            inner.detail,
      );
   }
   return {
      ok: true,
      joinedTerms: [
         { alias: label, source, terms: inner.terms },
         ...(inner.joinedTerms ?? []).map((nested) => ({
            ...nested,
            alias: `${label}.${nested.alias}`,
         })),
      ],
   };
}

/**
 * How many admitted given-scoped joins may nest. Real grant chains are one or
 * two deep; the bound stops a pathological model recursing without limit.
 */
const MAX_JOIN_DEPTH = 4;

/**
 * Whether compiled annotations declare `#@ persist … storage=<non-empty>`, read
 * the way Malloy's own `checkPersistAnnotation` reads `#@ persist`. Fails closed:
 * unreadable annotations do not declare storage.
 */
function declaresStorage(annotations: unknown): boolean {
   if (annotations === undefined || annotations === null) return false;
   try {
      const tag = parseRouteTag(
         new Annotations(
            annotations as ConstructorParameters<typeof Annotations>[0],
         ),
         "@",
      ).tag;
      if (!tag.has("persist")) return false;
      const storage = tag.text("storage");
      return typeof storage === "string" && storage.trim().length > 0;
   } catch {
      return false;
   }
}

/**
 * The single-segment field paths a filter entry reads, from its `refSummary`.
 * Any joined path (`["vis","user_id"]`, not a stored column) yields `[]`:
 * scoping a merge by a subset of a term's columns would match rows the term
 * excludes. See {@link DynamicTerm.columns}.
 */
function localFieldNames(entry: unknown): string[] {
   const usage = (
      entry as {
         refSummary?: { fieldUsage?: { path?: unknown }[] };
      }
   )?.refSummary?.fieldUsage;
   if (!Array.isArray(usage)) return [];
   const names = new Set<string>();
   for (const use of usage) {
      const path = use?.path;
      if (
         !Array.isArray(path) ||
         path.length !== 1 ||
         typeof path[0] !== "string"
      ) {
         // A joined path: report nothing rather than a subset of columns.
         return [];
      }
      names.add(path[0]);
   }
   return [...names];
}

/**
 * The source's own read-time dynamic terms, INDEPENDENT of whether
 * {@link admitGivenPositions} admits it. That classification returns no terms
 * on a refusal, and the colocated gate does not refuse the positional cases, so
 * reading terms off it would yield an empty ("no scoping needed") scope for a
 * caller-scoped source. Callers that must not fail open (the incremental merge
 * scope) read this instead.
 */
export function readTimeDynamicTerms(
   persistSource: PersistSource,
): DynamicTerm[] {
   try {
      const def = persistSource._sourceDef as unknown as {
         filterList?: unknown;
         fields?: unknown;
      };
      return givenTerms(classifyWhereTerms(def ?? {}));
   } catch {
      // Unreadable is not "none": a caller that cannot see the terms must treat
      // the source as unscopable, which one empty-column term expresses.
      return [{ code: "<unreadable>", givens: [], columns: [] }];
   }
}

/** A zero-based range in a Malloy file, as the compiler reports one. */
export interface MalloyRange {
   start: { line: number; character: number };
   end: { line: number; character: number };
}

/** Where a term reads a field or a given, as its `refSummary` records it. */
export interface TermUse {
   url: string;
   range: MalloyRange;
}

function termUse(use: unknown): TermUse | undefined {
   const at = (use as { at?: { url?: unknown; range?: unknown } } | null)?.at;
   const range = at?.range as MalloyRange | undefined;
   if (
      typeof at?.url !== "string" ||
      typeof range?.start?.line !== "number" ||
      typeof range?.start?.character !== "number" ||
      typeof range?.end?.line !== "number" ||
      typeof range?.end?.character !== "number"
   ) {
      return undefined;
   }
   return { url: at.url, range };
}

/**
 * Every place a filter entry reads a field or a given, as its `refSummary`
 * records them, and the compiler's ids of the givens it reads (keys of
 * `ModelDef.givens`). `at` is empty when any use carries no location: such a
 * term cannot be placed, and a caller that must remove it from the build has
 * to refuse rather than guess.
 */
function termLocations(entry: object): { at: TermUse[]; givenIds: string[] } {
   const summary = (
      entry as {
         refSummary?: { fieldUsage?: unknown; givenUsage?: unknown };
      }
   ).refSummary;
   const givenUsage = Array.isArray(summary?.givenUsage)
      ? summary.givenUsage
      : [];
   const uses = [
      ...(Array.isArray(summary?.fieldUsage) ? summary.fieldUsage : []),
      ...givenUsage,
   ].map(termUse);
   const givenIds = givenUsage
      .map((use) => (use as { id?: unknown } | null)?.id)
      .filter((id): id is string => typeof id === "string");
   return {
      givenIds: [...new Set(givenIds)],
      at:
         uses.length > 0 && uses.every((u) => u !== undefined)
            ? (uses as TermUse[])
            : [],
   };
}

/**
 * The `where:` terms a compiled source applies that read a given, as their
 * source text and the givens they read. These must be applied when a stored
 * table is read; a source served from its table that lost one would answer
 * every caller alike. A term whose text the compiler did not record has an
 * empty `code`, so a caller comparing texts fails closed on it.
 */
export function givenReadingFilterTerms(
   def: unknown,
): { code: string; givens: string[] }[] {
   const source = (def ?? {}) as { filterList?: unknown; fields?: unknown };
   return givenTerms(classifyWhereTerms(source)).map((t) => ({
      code: t.code,
      givens: [...t.givens].sort(),
   }));
}

/**
 * The given-reading terms of a classification, in `where:` order: every term
 * that reads a given, whether or not it also reads the current time. This is
 * what v1 persistence strips from the build and re-applies at read
 * (`strippedTerms`, the incremental merge scope, the merge-key check).
 */
export function givenTerms(
   classified: readonly ClassifiedWhereTerm[],
): DynamicTerm[] {
   const out: DynamicTerm[] = [];
   for (const { dependsOn } of classified) {
      if (
         dependsOn.kind === "given" ||
         dependsOn.kind === "given_and_current_time"
      ) {
         out.push(dependsOn.term);
      }
   }
   return out;
}

/**
 * One extend-block `where:` conjunct that reads a given, classified whole (an
 * `or` or range re-applies whole at read).
 *
 * {@link DynamicTerm.columns} ignores composition, so `a = $X or b = $Y`
 * contributes BOTH; a merge on `key AND a AND b` is narrower than the author's
 * disjunction. The error direction is a duplicate row, never another caller's
 * row; see the incremental section of `docs/materialization.md`.
 *
 * A term whose given names cannot be read is still returned, with none. Safe:
 * the shape re-emits every `filterList` entry, so the missing `given:`
 * declaration fails its compile and the binding is withheld (serves live).
 */
function dynamicTerm(entry: object): DynamicTerm {
   const code = (entry as { code?: unknown }).code;
   return {
      code: typeof code === "string" ? code : "",
      givens: givenNames(entry),
      columns: localFieldNames(entry),
   };
}

// ---------------------------------------------------------------------------
// The one `where:` term classifier (materialization v2 plan, Consolidation):
// every term of a compiled source by what it depends on beside the stored
// columns: nothing (fixed), a given, the current time, or both.
// ---------------------------------------------------------------------------

/**
 * A current-time comparison in the one shape a stored table can re-apply at
 * read: a one-segment field compared with `now`, optionally offset by a whole
 * number of one unit, read with the field on the left. The direction is the
 * comparison: `>=`/`>` keep rows newer than the cut-off (a lower bound),
 * `<`/`<=` rows older than it (an upper bound).
 */
export interface RollingWindow {
   /** The field the term compares (a stored column, when it is stored). */
   field: string;
   /** As read with the field on the left. */
   op: NowComparisonOp;
   /** `now + sign * amount unit`; absent for a bare `now`. */
   offset?: { sign: 1 | -1; amount: number; unit: NowOffsetUnit };
}

/** What one `where:` term of a compiled source depends on. */
type WhereTermDependency =
   /** Neither a given nor the current time: built once, never re-applied. */
   | { kind: "fixed" }
   /**
    * A given (P2): {@link DynamicTerm} as {@link dynamicTerm} reads it, with
    * where it reads each field and given (see {@link termLocations}).
    */
   | {
        kind: "given";
        term: DynamicTerm;
        at: TermUse[];
        givenIds: string[];
     }
   /**
    * The current time, directly or through a field or join that reads it.
    * `windows` is set only when the term is a {@link RollingWindow}, or an
    * `and` of them (a two-sided window), one per comparison in the order
    * written; `at` is then where the term reads each field, as for a given
    * term, so a build that leaves it out can find it.
    */
   | { kind: "current_time"; windows?: RollingWindow[]; at?: TermUse[] }
   /**
    * Both a given and the current time. `term` is the whole term as v1
    * persistence reads it (see {@link givenTerms}).
    */
   | { kind: "given_and_current_time"; term: DynamicTerm };

/** One `where:` term of a compiled source, classified. */
export interface ClassifiedWhereTerm {
   /** The author's text of the term, or "" when the compiler recorded none. */
   code: string;
   dependsOn: WhereTermDependency;
}

/**
 * Classify every `where:` term of a compiled source (its whole `filterList`,
 * inherited terms included: an extension's list carries its parent's) by what
 * it depends on. A term reads the current time when it spells `now`, or reads
 * a field of the source whose value depends on it ({@link currentTimeFields}).
 *
 * This is the one reader of a source's `where:` terms for materialization:
 * the build generator decides from it which terms build, which are re-applied
 * at read and which refuse the source, and v1 persistence reads its
 * given-reading subset ({@link givenTerms}).
 */
export function classifyWhereTerms(def: {
   filterList?: unknown;
   fields?: unknown;
}): ClassifiedWhereTerm[] {
   const list = Array.isArray(def.filterList) ? def.filterList : [];
   const timeFields = currentTimeFields(def.fields);
   const out: ClassifiedWhereTerm[] = [];
   for (const entry of list) {
      if (entry === null || typeof entry !== "object") continue;
      const rawCode = (entry as { code?: unknown }).code;
      const code = typeof rawCode === "string" ? rawCode : "";
      const e = (entry as { e?: unknown }).e;
      const readsTime = readsCurrentTime(e) || readsTimeField(e, timeFields);
      const readsGiven = readsAGiven(entry, new WeakSet());
      if (readsTime && readsGiven) {
         out.push({
            code,
            dependsOn: {
               kind: "given_and_current_time",
               term: dynamicTerm(entry),
            },
         });
      } else if (readsGiven) {
         out.push({
            code,
            dependsOn: {
               kind: "given",
               term: dynamicTerm(entry),
               ...termLocations(entry),
            },
         });
      } else if (readsTime) {
         // A window over a per-read field reads no stored column; the
         // caller's stored-column check refuses it.
         const windows = readRollingWindows(entry);
         out.push({
            code,
            dependsOn: windows
               ? { kind: "current_time", windows, at: termLocations(entry).at }
               : { kind: "current_time" },
         });
      } else {
         out.push({ code, dependsOn: { kind: "fixed" } });
      }
   }
   return out;
}

/** Whether a slice of compiled IR reads the current time (`now`). */
export function readsCurrentTime(node: unknown, depth = 0): boolean {
   if (depth > MAX_WALK_DEPTH) return true; // fail closed
   if (node === null || typeof node !== "object") return false;
   if (Array.isArray(node)) {
      return node.some((item) => readsCurrentTime(item, depth + 1));
   }
   const record = node as Record<string, unknown>;
   if (record.node === "now") return true;
   for (const [key, value] of Object.entries(record)) {
      // Locations and annotations carry no expression.
      if (key === "at" || key === "location" || key === "annotations") continue;
      if (readsCurrentTime(value, depth + 1)) return true;
   }
   return false;
}

const COMPARISONS: ReadonlySet<string> = new Set([">=", ">", "<", "<="]);
const MIRROR: Record<NowComparisonOp, NowComparisonOp> = {
   ">=": "<=",
   ">": "<",
   "<": ">",
   "<=": ">=",
};

interface NowExpr {
   node?: unknown;
   kids?: Record<string, unknown>;
   path?: unknown;
   op?: unknown;
   units?: unknown;
   literal?: unknown;
}

/** `now` or `now ± <n> <unit>`, read as an offset; undefined for any other shape. */
function readNow(
   e: NowExpr | undefined,
): { offset?: RollingWindow["offset"] } | undefined {
   if (!e || typeof e !== "object") return undefined;
   if (e.node === "now") return {};
   if (e.node !== "delta") return undefined;
   const base = e.kids?.base as NowExpr | undefined;
   const delta = e.kids?.delta as NowExpr | undefined;
   if (base?.node !== "now") return undefined;
   if (delta?.node !== "numberLiteral" || typeof delta.literal !== "string") {
      return undefined;
   }
   if (!/^\d+$/.test(delta.literal)) return undefined;
   const amount = Number(delta.literal);
   if (!Number.isSafeInteger(amount)) return undefined;
   if (e.op !== "+" && e.op !== "-") return undefined;
   if (
      typeof e.units !== "string" ||
      !(NOW_OFFSET_UNITS as readonly string[]).includes(e.units)
   ) {
      return undefined;
   }
   return {
      offset: {
         sign: e.op === "-" ? -1 : 1,
         amount,
         unit: e.units as NowOffsetUnit,
      },
   };
}

/** A one-segment field reference's name, or undefined. */
function readField(e: NowExpr | undefined): string | undefined {
   if (!e || e.node !== "field" || !Array.isArray(e.path)) return undefined;
   if (e.path.length !== 1 || typeof e.path[0] !== "string") return undefined;
   return e.path[0];
}

/** One filter entry as a {@link RollingWindow}, or undefined when its shape is not one. */
export function readRollingWindow(entry: unknown): RollingWindow | undefined {
   const e = (entry as { e?: NowExpr } | null)?.e;
   if (!e || typeof e.node !== "string" || !COMPARISONS.has(e.node)) {
      return undefined;
   }
   const left = e.kids?.left as NowExpr | undefined;
   const right = e.kids?.right as NowExpr | undefined;
   const op = e.node as NowComparisonOp;
   const leftField = readField(left);
   const rightNow = readNow(right);
   if (leftField !== undefined && rightNow) {
      return { field: leftField, op, ...rightNow };
   }
   const rightField = readField(right);
   const leftNow = readNow(left);
   if (rightField !== undefined && leftNow) {
      return { field: rightField, op: MIRROR[op], ...leftNow };
   }
   return undefined;
}

/**
 * One filter entry as its {@link RollingWindow}s: one for a single comparison,
 * one per conjunct for an `and` whose every conjunct is one (a two-sided
 * window, `ts >= now - 60 days and ts < now - 30 days`, parenthesised or
 * not, or Malloy's range `ts ? now - 60 days to now - 30 days`, which compiles
 * to the same `and`), or undefined for any other shape.
 */
function readRollingWindows(entry: unknown): RollingWindow[] | undefined {
   const e = (entry as { e?: NowExpr } | null)?.e;
   const out: RollingWindow[] = [];
   const walk = (node: NowExpr | undefined, depth: number): boolean => {
      if (!node || depth > MAX_WALK_DEPTH) return false;
      if (node.node === "()") {
         return walk((node as { e?: NowExpr }).e, depth + 1);
      }
      if (node.node === "and") {
         return (
            walk(node.kids?.left as NowExpr | undefined, depth + 1) &&
            walk(node.kids?.right as NowExpr | undefined, depth + 1)
         );
      }
      const window = readRollingWindow({ e: node });
      if (!window) return false;
      out.push(window);
      return true;
   };
   return walk(e, 0) ? out : undefined;
}

/**
 * The dotted field paths an expression reads; one inside an array query is
 * re-rooted under its array.
 */
function fieldPaths(node: unknown, out: Set<string>): void {
   forEachFieldPath(
      node,
      (path) => {
         if (path.length > 0) out.add(path.join("."));
      },
      { skipKeys: FIELD_PATH_SKIPPED_KEYS, maxDepth: MAX_WALK_DEPTH },
   );
}

const FIELD_PATH_SKIPPED_KEYS = new Set(["at", "location", "annotations"]);

/**
 * Whether a dotted path names a field in `timeFields`, or reads through a join
 * there (a join whose own `where:` or `on` reads the current time makes every
 * value read through it depend on it).
 */
function dependsOnCurrentTime(
   path: string,
   timeFields: ReadonlySet<string>,
): boolean {
   const parts = path.split(".");
   for (let i = 1; i <= parts.length; i++) {
      if (timeFields.has(parts.slice(0, i).join("."))) return true;
   }
   return false;
}

/** Whether an expression reads a field whose value depends on the current time. */
function readsTimeField(e: unknown, timeFields: ReadonlySet<string>): boolean {
   if (timeFields.size === 0) return false;
   const paths = new Set<string>();
   fieldPaths(e, paths);
   return [...paths].some((p) => dependsOnCurrentTime(p, timeFields));
}

/**
 * Every field of a source whose value depends on the current time, as a dotted
 * path: it reads `now`, or reads (through any chain of the source's fields,
 * joined ones included) one that does. A join whose own `where:` or `on` reads
 * the current time is listed by its own name, since every value read through
 * it depends on it. Such a dimension is never stored, since its value differs
 * per read.
 */
export function currentTimeFields(fields: unknown): Set<string> {
   return fieldsDependingOn(fields, {
      field: (f) => readsCurrentTime(f.e),
      join: (f) =>
         readsCurrentTime(f.filterList) || readsCurrentTime(f.onExpression),
   });
}

/**
 * Every field of a source whose value reads a given, as a dotted path, by the
 * same rule as {@link currentTimeFields}: it reads one itself (the IR walk
 * {@link readsAGiven}, which also reads Malloy's `givenUsage` summary), or
 * reads a field or join that does. P9's IR signal, beside the probe
 * compiler's: such a dimension is never stored, since its value differs per
 * caller. Fails closed: a field the walk cannot finish is listed.
 */
export function givenReadingFields(fields: unknown): Set<string> {
   const reads = (node: unknown): boolean => {
      try {
         return readsAGiven(node);
      } catch {
         return true;
      }
   };
   return fieldsDependingOn(fields, {
      field: (f) => reads(f),
      join: (f) => reads(f.filterList) || reads(f.onExpression),
   });
}

interface FieldLikeForDependence {
   name?: unknown;
   as?: unknown;
   e?: unknown;
   join?: unknown;
   fields?: unknown;
   filterList?: unknown;
   onExpression?: unknown;
}

/**
 * The fields of a source that depend on something, transitively: a field that
 * `reads.field` marks, a join `reads.join` marks (every path through it), the
 * same inside each joined source, and any field or join `on` that reads one
 * of those.
 */
function fieldsDependingOn(
   fields: unknown,
   reads: {
      field: (f: FieldLikeForDependence) => boolean;
      join: (f: FieldLikeForDependence) => boolean;
   },
   depth = 0,
): Set<string> {
   const out = new Set<string>();
   if (!Array.isArray(fields) || depth > MAX_WALK_DEPTH) return out;
   const deps = new Map<string, Set<string>>();
   for (const raw of fields) {
      const f = raw as FieldLikeForDependence;
      const name =
         typeof f.as === "string"
            ? f.as
            : typeof f.name === "string"
              ? f.name
              : undefined;
      if (name === undefined) continue;
      const refs = new Set<string>();
      if (f.join !== undefined) {
         if (reads.join(f)) {
            out.add(name);
            continue;
         }
         for (const inner of fieldsDependingOn(f.fields, reads, depth + 1)) {
            out.add(`${name}.${inner}`);
         }
         // An `on` that reads one of this source's dependent dimensions.
         fieldPaths(f.onExpression, refs);
      } else {
         if (reads.field(f)) out.add(name);
         fieldPaths(f.e, refs);
      }
      deps.set(name, refs);
   }
   for (let changed = true; changed; ) {
      changed = false;
      for (const [name, refs] of deps) {
         if (out.has(name)) continue;
         if ([...refs].some((r) => dependsOnCurrentTime(r, out))) {
            out.add(name);
            changed = true;
         }
      }
   }
   return out;
}
