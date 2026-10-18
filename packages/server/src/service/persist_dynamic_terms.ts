// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Annotations, type PersistSource } from "@malloydata/malloy";
import { collectGivenRefs } from "./given";

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
 * Whether a slice of IR reads a given, by EITHER a `{node:'given'}` reference
 * or a non-empty `givenUsage` summary, so a compiler that prunes one of the two
 * signals cannot make a given read clean.
 */
function readsAGiven(node: unknown, seen: WeakSet<object>, depth = 0): boolean {
   if (depth > MAX_WALK_DEPTH) {
      throw new Error("given walk exceeded max depth");
   }
   if (node === null || typeof node !== "object") return false;
   if (seen.has(node as object)) return false;
   seen.add(node as object);

   if (Array.isArray(node)) {
      return node.some((item) => readsAGiven(item, seen, depth + 1));
   }
   const record = node as Record<string, unknown>;
   if (record.node === "given" || record.node === "givenReference") return true;
   if (record.givenRef !== undefined) return true;
   const summary = record.refSummary as { givenUsage?: unknown[] } | undefined;
   if (summary?.givenUsage && summary.givenUsage.length > 0) return true;
   if (Array.isArray(record.givenUsage) && record.givenUsage.length > 0) {
      return true;
   }
   return Object.values(record).some((v) => readsAGiven(v, seen, depth + 1));
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
 * Classify every given a persist source reads by POSITION, returning the
 * extend-block `where:` terms that may be stripped from the build and
 * re-applied at read, or the first position that cannot be.
 *
 * The baked check runs FIRST: a baked given is the one position no serve shape
 * can rescue, so reporting anything else would send the author to the wrong
 * line. Fails closed: a given in a position this pass does not know is
 * refused, since the failure mode is serving one tenant's rows to another.
 */
export function classifyDynamicTerms(
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
 * {@link classifyDynamicTerms} over a compiled source definition. Separate so a
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

      const terms = collectDynamicTerms(record.filterList);

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
      const tag = new Annotations(
         annotations as ConstructorParameters<typeof Annotations>[0],
      ).parseAsTag("@").tag;
      if (!tag.has("persist")) return false;
      const storage = tag.text("storage");
      return typeof storage === "string" && storage.trim().length > 0;
   } catch {
      return false;
   }
}

/**
 * The `sourceID`s of the given-scoped sources a field list joins. A join that
 * can never be re-emitted is `undefined`: an inline refinement (no `sourceID`)
 * or a non-public join, which `extractJoins` never re-emits while a public
 * field may still read through it.
 */
export function givenScopedJoinTargets(
   fields: unknown,
): (string | undefined)[] {
   if (!Array.isArray(fields)) return [];
   const out: (string | undefined)[] = [];
   for (const field of fields) {
      if (field === null || typeof field !== "object") continue;
      const f = field as Record<string, unknown>;
      if (f.join === undefined || !readsAGiven(f, new WeakSet())) continue;
      const restricted =
         f.accessModifier != null && f.accessModifier !== "public";
      out.push(
         !restricted && typeof f.sourceID === "string" && f.sourceID.length > 0
            ? f.sourceID
            : undefined,
      );
   }
   return out;
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
 * {@link classifyDynamicTerms} admits it. That classification returns no terms
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
      };
      return collectDynamicTerms(def?.filterList);
   } catch {
      // Unreadable is not "none": a caller that cannot see the terms must treat
      // the source as unscopable, which one empty-column term expresses.
      return [{ code: "<unreadable>", givens: [], columns: [] }];
   }
}

/**
 * The extend-block `where:` conjuncts that reference a given, each classified
 * whole (an `or` or range re-applies whole at read).
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
function collectDynamicTerms(filterList: unknown): DynamicTerm[] {
   if (!Array.isArray(filterList)) return [];
   const terms: DynamicTerm[] = [];
   for (const entry of filterList) {
      if (entry === null || typeof entry !== "object") continue;
      if (!readsAGiven(entry, new WeakSet())) continue;
      const code = (entry as { code?: unknown }).code;
      terms.push({
         code: typeof code === "string" ? code : "",
         givens: givenNames(entry),
         columns: localFieldNames(entry),
      });
   }
   return terms;
}
