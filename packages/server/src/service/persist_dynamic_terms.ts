// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { PersistSource } from "@malloydata/malloy";
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
 * than answering yes/no: of the read-time positions, only an extend-block
 * `where:` is admitted in v1. The others are not refused because they are
 * unsafe — they are not in the build either — but because admitting them is a
 * separate decision with its own serve-shape questions, and this pass refuses
 * what it has not proven. Each gets its own reason so the refusal names the
 * position rather than the mechanism.
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

export type DynamicTermClassification =
   | { ok: true; terms: DynamicTerm[] }
   | { ok: false; reason: DynamicTermRefusal; detail: string };

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
   try {
      const def = persistSource._sourceDef as unknown;
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
   if (buildSubstitutesAGiven(persistSource)) {
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

   let def: Record<string, unknown>;
   try {
      def = persistSource._sourceDef as unknown as Record<string, unknown>;
      if (def === null || typeof def !== "object") {
         throw new Error("compiled source definition is not readable");
      }
   } catch (err) {
      return {
         ok: false,
         reason: "given_in_persisted_query",
         detail:
            `its given usage could not be determined ` +
            `(${err instanceof Error ? err.message : String(err)}), so the ` +
            `publisher cannot prove which givens the build would bake in`,
      };
   }

   try {
      const fieldRefusal = classifyFields(def.fields);
      if (fieldRefusal) return fieldRefusal;

      const terms = collectDynamicTerms(def.filterList);

      // Fail-closed sweep. `filterList` and `fields` were just accounted for
      // above, one position at a time; `query` was cleared by the baked check.
      // A given anywhere ELSE is one this pass has no placement for, so it is
      // refused rather than assumed harmless.
      const {
         filterList: _accounted,
         fields: _alsoAccounted,
         query: _cleared,
         ...rest
      } = def;
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
      return { ok: true, terms };
   } catch (err) {
      return {
         ok: false,
         reason: "given_in_persisted_query",
         detail:
            `its given usage could not be determined ` +
            `(${err instanceof Error ? err.message : String(err)}), so the ` +
            `publisher cannot prove the build leaves every given out`,
      };
   }
}

/**
 * The first field position carrying a given that v1 does not admit, or
 * undefined when the source's fields read none.
 *
 * None of these is baked into the artifact — the probe table in this module's
 * header shows all three absent from the build SQL — so none is a leak today.
 * They are refused because admitting them is a decision about the SERVE shape
 * (a re-emitted dimension, join or joined filter binds the given per caller
 * only if the shape reproduces it, and the shape-compile ladder may drop a
 * category), and that decision has not been taken. A refusal here costs
 * coverage; admitting one wrongly costs isolation.
 */
function classifyFields(
   fields: unknown,
): { ok: false; reason: DynamicTermRefusal; detail: string } | undefined {
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
         // target's fields. One reason covers them, because the remedy is the
         // same — the given-scoped source is joined IN rather than entered
         // through, so it cannot be a term this source strips.
         if (readsAGiven(f, new WeakSet())) {
            return {
               ok: false,
               reason: "dynamic_joined_where",
               detail:
                  `the source joined as '${label}' is itself scoped by a ` +
                  `given. Enter through a non-persisted extension that ` +
                  `declares the join instead, so the given term is part of ` +
                  `the query rather than of the artifact`,
            };
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
 * The extend-block `where:` conjuncts that reference a given — the terms left
 * out of the build and re-applied at read.
 *
 * `filterList` entries are conjunctive and each is re-emitted as its own
 * `where:` line, so a term is classified whole: an `or`-composition or a range
 * is a dynamic term like any other, and re-applies whole. Only whether it
 * PRUNES depends on its shape, and that is the partition list's question, not
 * this one.
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
