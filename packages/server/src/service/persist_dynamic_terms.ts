// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Annotations, type PersistSource } from "@malloydata/malloy";
import { collectGivenRefs } from "./given";

/**
 * Where a persist source's givens sit, and whether that lets the source be
 * materialized into a storage destination and served per caller.
 *
 * The rule the whole module turns on, established by probing Malloy's own
 * persistence rather than by reading it:
 *
 *   A persist source's build SQL is the persisted relation ALONE. Its own
 *   `filterList` (the extend-block `where:`) and its own `fields` (the
 *   extend-block `dimension:` / `measure:` / `join:`) are NOT in it. They refine
 *   the relation when it is READ.
 *
 * So a given in one of those positions is never frozen into the artifact: the
 * artifact holds every caller's rows, and the term is re-applied at read with
 * the caller's own value — which is what the colocated tier has always done
 * (substitution swaps only the `FROM`, leaving the reading query its own
 * `WHERE`) and what the storage tier's serve shape does once it re-emits the
 * refinement and declares the given.
 *
 * A given ANYWHERE ELSE is substituted while the build runs, at the only value
 * available then — the declaration default — so the artifact holds one caller's
 * slice and every other caller is served it. That is {@link buildSubstitutesAGiven},
 * and it is a hard refusal on both tiers.
 *
 * Between those two lies the reason this module classifies by POSITION rather
 * than answering yes/no: of the read-time positions, two are admitted — an
 * extend-block `where:`, and a join to a given-scoped source that is itself
 * materialized into storage (see {@link JoinedDynamicTerms}). The others are not
 * refused because they are unsafe — they are not in the build either — but
 * because the serve shape has nothing that re-binds the given per caller in that
 * position. Each gets its own reason so the refusal names the position rather
 * than the mechanism.
 */
export type DynamicTermRefusal =
   | "given_in_persisted_query"
   | "dynamic_projection"
   | "dynamic_join"
   | "dynamic_joined_where";

/**
 * One extend-block `where:` conjunct that references a given: absent from the
 * build, re-applied at read. `code` is the author's verbatim expression text,
 * which is what the serve shape re-emits; `givens` are the names it reads,
 * which is what the serve shape must declare for that text to compile.
 */
export interface DynamicTerm {
   code: string;
   givens: string[];
   /**
    * The source's OWN columns this term constrains — single-segment field paths
    * only. A term reaching through a join contributes nothing here, which is
    * what makes an empty list meaningful: it says the term cannot be expressed
    * as a predicate over the stored table's columns.
    *
    * Read by the incremental apply, which must scope a `merge_key=` match by the
    * columns the stripped terms constrain. Not on the wire.
    */
   columns: string[];
}

/**
 * A join, declared on the persisted source, to a source that is itself scoped by
 * a given — admitted because the per-caller filtering rides the JOINED source's
 * own artifact rather than this one.
 *
 * The build never reads it (a persist source's `fields` are not in its build
 * SQL), so this artifact is unaffected. At read, the serve shape re-emits the
 * join only when `source` is itself bound, and that binding re-applies `terms`
 * with the caller's values — so a field reading through `alias` answers per
 * caller. When `source` is not bound the join cannot be re-emitted, so the
 * joining source is withheld from the shape with it and serves live.
 *
 * `alias` is dotted for a join reached through another admitted join
 * (`grant.team`), since the same rule holds one level further down.
 */
export interface JoinedDynamicTerms {
   alias: string;
   source: string;
   terms: DynamicTerm[];
}

export type DynamicTermClassification =
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
 * Whether a slice of IR reads a given, by EITHER of Malloy's two signals — a
 * `{node:'given'}` reference node, or a non-empty `refSummary.givenUsage` /
 * bare `givenUsage` summary. Both, because a future compiler keeping only the
 * summary and pruning the reference leaves would otherwise read clean.
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
 * The compiler answers this directly for a query-shaped source: a query's
 * `givenUsage` is the transitive summary of the givens its pipeline actually
 * reads, so a non-empty one means a value was substituted while producing the
 * persisted relation. Verified against `getSQL()` — `where:` inside the
 * persisted query, a given in a `group_by`, and the INPUT source's own `where:`
 * all carry the predicate into the build SQL and all report through this
 * marker; the persist source's own extend-block `where:`, `dimension:`,
 * `measure:` and `join:` carry none of it and report empty.
 *
 * A source that is not query-shaped (`base extend { where: … }`) has no `query`
 * at all, and its build SQL is the base relation unfiltered — so the structural
 * fallback answers instead, over everything BUT the two read-time keys.
 *
 * Fails closed: unreadable IR is a refusal.
 *
 * Mirrors the check publisher #1185 adds for the colocated tier. When that
 * lands the two become one function; the storage tier needs the same question
 * answered, and for a sharper reason — on colocated the baked shape builds and
 * lies, on storage it cannot be re-applied at read at all.
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
 * Whether a given is bound into the relation as a source ARGUMENT.
 *
 * `pp(x is $ORG_ID) -> { … }` substitutes the given while constructing the
 * source the query reads, so the predicate lands in the build SQL — byte
 * identical to writing the given inside the query. It binds at `structRef`
 * construction rather than by being referenced, so the `givenUsage` marker
 * reads empty while the build bakes the default.
 *
 * Any `arguments` holder anywhere under the query is searched, not just the
 * query's own. That descent is load-bearing rather than defensive: a given
 * binding a JOINED source declared on the input sits under `structRef.fields`,
 * and the marker reads empty for it even when the query reads the join and the
 * build SQL carries the predicate. Checking only the query's own holders admits
 * that shape — measured. A chain of parameterized sources nests one inside the
 * next for the same reason.
 *
 * It has a cost, accepted deliberately: the same base with a parameterized
 * given-filtered join the query does NOT read is refused too, though nothing
 * reaches the build. That is the over-refusal the marker approach exists to
 * avoid, reappearing in the one place the marker cannot see. Refusing a source
 * that does not bake loses a tier; admitting one that does loses a tenant's
 * isolation, so the descent stays until a signal precise enough to tell the two
 * apart exists.
 *
 * A CONSTANT argument bakes too and is left alone: that is a concrete
 * instantiation with no per-caller binding, which is the shape
 * `parameter-eligibility` admits.
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
 * Classify every given a persist source reads by the POSITION it sits in, and
 * return the extend-block `where:` terms that may be stripped from the build
 * and re-applied at read — or the first position that cannot be.
 *
 * Order matters and is not arbitrary. The baked check runs FIRST, because a
 * baked given is the one position where the artifact is already wrong and no
 * serve-shape work could rescue it; reporting a projection instead would send
 * an author to the wrong line. Everything after it concerns read-time positions
 * that are merely not admitted yet.
 *
 * Fail-closed at the end: any given left over — one this pass could not place
 * in a position it knows — is refused. New IR that carries a given somewhere
 * unanticipated therefore reads as a refusal rather than as an admission, which
 * is the only safe direction for a gate whose failure mode is serving one
 * tenant's rows to another.
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

      // Fail-closed sweep. `filterList` and `fields` were just accounted for
      // above, one position at a time; `query` was cleared by the baked check.
      // A given anywhere ELSE is one this pass has no placement for, so it is
      // refused rather than assumed harmless.
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
 * The first field position carrying a given that is not admitted, or undefined
 * when every given the source's fields read sits somewhere the serve shape
 * reproduces per caller. Admitted given-scoped joins are appended to
 * `joinedTerms`.
 *
 * None of these positions is baked into the artifact — a persist source's
 * `fields` are absent from its build SQL — so none is a leak at build time. What
 * decides admission is the SERVE shape: a re-emitted field binds the given per
 * caller only if the shape reproduces everything it reads.
 *
 * A join to a given-scoped source is the one field position that does. The
 * shape re-emits a join only when its target is itself bound
 * (`extractJoins`), and the target's binding re-emits the target's own
 * `where:` — so the join reaches a per-caller-filtered table, which is what the
 * live query joins. When the target is not bound the join is not re-emitted,
 * and anything reading through it fails the shape compile and serves live. Both
 * halves need the target to be a source this gate would itself admit to a
 * storage destination, JOINED BY NAME; see {@link joinedSourceRefusal}.
 *
 * Still refused: a given in a join's `on:` (`dynamic_join`), which decides per
 * caller which rows join and is re-emitted by nothing that binds it; and a
 * dimension or measure that reads a given itself (`dynamic_projection`). A field
 * that reads a given only THROUGH an admitted join carries no given of its own
 * — Malloy's summaries do not propagate one across a join path — and is
 * re-emitted like any other field.
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
 * The compiled keys a join field carries beyond the joined source's own
 * definition. Stripped before the target is classified, so what is classified is
 * the target as it would be persisted: the `on:` was checked separately, and the
 * join's `refSummary` summarises that condition and the target together.
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
 * target re-applies when it can.
 *
 * Three conditions, each closing a way the serve shape would otherwise fail to
 * reproduce the join. All three failing modes are fail-closed at serve time —
 * the join is not re-emitted and queries through it serve live — so they are
 * refused here to name the reason, not to prevent a leak:
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
 * two deep; the bound keeps a pathological model from recursing through every
 * joined definition it embeds.
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
 * The `sourceID`s of the given-scoped sources a field list joins — the targets
 * {@link joinedSourceRefusal} admits, and the ones whose absence from a serve
 * shape leaves a join that cannot be reproduced. A join to an inline refinement
 * has no `sourceID` and is reported as `undefined`, since nothing could bind it.
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
      out.push(
         typeof f.sourceID === "string" && f.sourceID.length > 0
            ? f.sourceID
            : undefined,
      );
   }
   return out;
}

/**
 * The extend-block `where:` conjuncts that reference a given — the terms left
 * out of the build and re-applied at read.
 *
 * `filterList` entries are conjunctive and each is re-emitted as its own
 * `where:` line, so a term is classified whole: an `or`-composition or a range
 * is a dynamic term like any other, and re-applies whole.
 *
 * That holds for the READ, which is what this classification is for. It does not
 * hold for {@link DynamicTerm.columns}, which the merge scope reads: the columns
 * are collected from the term's field usage without regard to how they compose,
 * so `a = $X or b = $Y` contributes BOTH. A merge then matches on
 * `key AND a AND b`, which is narrower than the disjunction the author wrote —
 * the error direction is a row that fails to match its stored copy and is
 * inserted beside it, never a row belonging to another caller. Safe, and a
 * duplicate; see the incremental section of `docs/materialization.md`.
 *
 * A term whose given references cannot be read is still returned, with no
 * names. That is deliberate and safe: the term is re-emitted onto the serve
 * shape either way (the shape carries every `filterList` entry), so an
 * under-named term compiles against a shape missing its `given:` declaration
 * and the binding is WITHHELD. The failure is a fallback to live, not an
 * unfiltered read.
 */
/**
 * The single-segment field paths a filter entry reads, from its `refSummary`.
 *
 * Single-segment ONLY, and the omission is the point: `["vis","user_id"]` is a
 * joined field, which is not a column of the stored table and so cannot scope a
 * merge over it. Dropping it leaves the term with fewer columns than it reads,
 * which a caller must treat as "cannot scope this" rather than as a smaller
 * scope — scoping by a subset of a term's columns would match rows the term
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
         // A reference this cannot express as a column of THIS source — a joined
         // path. Reporting the term's OTHER columns would scope by a subset of
         // what the term constrains, giving a match narrower than the bare key
         // and still wider than the author's relation. An empty list is how a
         // term says it cannot be scoped by at all.
         return [];
      }
      names.add(path[0]);
   }
   return [...names];
}

/**
 * The source's own read-time dynamic terms, INDEPENDENT of whether the positional
 * classification admits the source.
 *
 * {@link classifyDynamicTerms} answers "may this source be materialized", and
 * returns no terms when the answer is no. That is the right shape for a gate and
 * the wrong shape for anything that has to be correct about a source the gate
 * did not refuse — and the two gates refuse different sets. The colocated gate
 * does not refuse the positional cases (`dynamic_projection`, `dynamic_join`,
 * `dynamic_joined_where`), so reading terms off the classification there yields
 * an empty scope for a source that is nonetheless caller-scoped, and an empty
 * scope silently means "no scoping needed".
 *
 * Callers that must not fail open — the incremental merge scope — read this
 * instead, so their correctness does not depend on which gate ran or on what it
 * happened to refuse for an unrelated reason.
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
