import type { PersistSource } from "@malloydata/malloy";
import { MaterializationEligibilityError } from "../errors";
import { recordEligibilityRefused } from "../materialization_metrics";
import { parseAuthorizeAnnotation } from "./authorize";

/**
 * Compile-time eligibility gate for materializing a persist source into a
 * *storage* destination (a DuckDB/DuckLake tier table), as opposed to the
 * default colocated path (no `storage=`). This is a HARD REFUSE: an
 * ineligible source never builds an artifact.
 *
 * It exists because a `storage=<duckdb>` table is built once and served frozen
 * to every subsequent query. Two properties that hold for a live query — the
 * relation is recomputed per query, and per-tenant access filters bind at query
 * time — are lost the moment the relation is frozen. So a source is only
 * materializable when its relation is fully determined at build time:
 *
 *  1. **No unbound (free) parameters.** A source declaring an unbound parameter
 *     is a *template* instantiated per query — there is no single relation to
 *     freeze. Parameters bound to a constant are fine (the relation is fixed,
 *     and the bound value already distinguishes the content address).
 *  2. **No given references — a security refusal.** Givens bind at the
 *     runtime/query layer and are the documented mechanism for row-level access
 *     control (RLAC). A source filtered by a given (`where: tenant_id = $TENANT`)
 *     materialized once and served frozen would leak one tenant's rows to every
 *     tenant. This check fails closed: if the source references any given, it is
 *     refused, no exceptions.
 *  3. **No `#(authorize)` gate — a security refusal.** An authorize expression
 *     is a per-request *who-can-query* gate evaluated at query time. The served
 *     virtual shape of a materialized source carries no gate to evaluate, so a
 *     materialized authorize-gated source would be served to everyone,
 *     bypassing the gate. Fails closed on anything it cannot read.
 *
 *     Its join reach is PARTIAL, and the limit is worth knowing before relying on
 *     it. The scan is a blind deep walk for an authorize annotation anywhere in
 *     the compiled source def, so it does find a gate on a plainly-joined source
 *     and one filed under `annotations.inherits`. It does NOT find a gate on a
 *     source reached through an ANNOTATED join: Malloy replaces an annotated
 *     `join_*`'s target annotations outright, leaving no `inherits` and no
 *     authorize byte in the subtree, and the only surviving link is a
 *     `sourceID` into `ModelDef.sourceRegistry` — which this pass has no modelDef
 *     to resolve. Measured: `join_one: base_locked` refuses, the same join under
 *     a `# render_tag` is eligible.
 *
 *     No exposure follows from that today: the serve path does not gate joins
 *     either (a gate is evaluated at the entry point only), so a frozen table
 *     grants nothing a live query would not. The gap matters if the serve path
 *     ever starts tracing joins again, or if this scan is treated as the reason
 *     joined-in gated data is safe to freeze. It is not that reason yet.
 *
 * One further eligibility property from the design — the served source must
 * compile in DuckDB (portability) — is enforced at *build* time against the
 * captured table schema (see the DuckDB-compile gate in the build path), not
 * here: it needs the post-build authoritative schema this compile-time pass
 * does not have. This pass covers the properties determinable from the compiled
 * source alone (free parameters, given references, authorize gates), so it can
 * fail fast before any warehouse work.
 *
 * The gate reads the *compiled* source (only the compiled `ModelDef` sees
 * effective annotations, parameters, and given usage through `extend`/imports —
 * a raw-source scan would miss inherited declarations), so it must run after
 * compilation and before the build.
 *
 * @throws {MaterializationEligibilityError} (HTTP 422) naming the source and the
 *   specific reason, so the author can either fix the source or drop `storage=`.
 */
export function assertMaterializationEligible(
   persistSource: PersistSource,
): void {
   const sourceName = persistSource.name;

   // Fail closed, like referencesGiven / referencesAuthorize below: an unreadable
   // parameter surface is a refusal, not an assumed absence of free parameters.
   let unbound: string[];
   try {
      unbound = unboundParameterNames(persistSource);
   } catch (err) {
      recordEligibilityRefused("free_parameter");
      throw new MaterializationEligibilityError({
         message:
            `Source '${sourceName}' cannot be materialized into a storage ` +
            `destination: its parameter surface could not be determined ` +
            `(${err instanceof Error ? err.message : String(err)}), so the ` +
            `publisher cannot prove the source has no free parameters. This is ` +
            `refused for safety. Drop 'storage=' to serve it live from the ` +
            `source warehouse.`,
      });
   }
   if (unbound.length > 0) {
      recordEligibilityRefused("free_parameter");
      throw new MaterializationEligibilityError({
         message:
            `Source '${sourceName}' cannot be materialized into a storage ` +
            `destination: it has unbound parameter(s) ` +
            `${unbound.map((p) => `'${p}'`).join(", ")}. A source with a free ` +
            `parameter is a template instantiated per query, so there is no ` +
            `single relation to materialize. Bind the parameter to a constant, ` +
            `or drop 'storage=' to serve it live from the source warehouse.`,
      });
   }

   if (referencesGiven(persistSource)) {
      recordEligibilityRefused("given");
      throw new MaterializationEligibilityError({
         message:
            `Source '${sourceName}' cannot be materialized into a storage ` +
            `destination: it references a given. Givens bind per query and are ` +
            `used for row-level access control, so a materialized-once table ` +
            `served to everyone would leak filtered rows across tenants. This ` +
            `is refused for safety. Serve this source live (drop 'storage=').`,
      });
   }

   if (referencesAuthorize(persistSource)) {
      recordEligibilityRefused("authorize");
      throw new MaterializationEligibilityError({
         message:
            `Source '${sourceName}' cannot be materialized into a storage ` +
            `destination: it is protected by an #(authorize) gate (its own or a ` +
            `joined source's). An authorize expression is evaluated per request; ` +
            `a materialized-once table served frozen carries no gate, so it would ` +
            `be served to everyone, bypassing authorization. This is refused for ` +
            `safety. Serve this source live (drop 'storage=').`,
      });
   }
}

/**
 * Compile-time eligibility gate for the COLOCATED persist path (a plain
 * `#@ persist` with no `storage=`, which builds a CTAS into the source's own
 * warehouse). Deliberately narrow: it checks ONLY the `#(authorize)` condition
 * from {@link assertMaterializationEligible}, reusing the same `referencesAuthorize`
 * walk rather than duplicating it, and does NOT apply that function's other
 * rules (`referencesGiven`, unbound parameters). Those other rules exist
 * because a *storage destination* — a separate DuckDB/DuckLake table — cannot
 * represent a per-query given or a free parameter; a colocated build has no
 * such constraint (it is still one relation per source, computed once, in the
 * source's own warehouse), so applying them here would refuse a large set of
 * packages that build and serve correctly today. Only the authorize gate is a
 * problem on this path: it is evaluated per request, and a colocated build is
 * just as frozen as a storage build, so the same leak applies.
 *
 * @throws {MaterializationEligibilityError} (HTTP 422) naming the source and
 *   the remedy, so the author can either drop `#@ persist` from this source or
 *   move the gate to a source that is not materialized.
 */
export function assertColocatedPersistNotAuthorizeGated(
   persistSource: PersistSource,
   sourceName: string = persistSource.name,
): void {
   if (referencesAuthorize(persistSource)) {
      // Reuses the "authorize" reason: this is the same underlying refusal
      // (a frozen table serving an authorize-gated relation to everyone) as
      // the storage-destination case, just reached via the colocated path.
      recordEligibilityRefused("authorize");
      throw new MaterializationEligibilityError({
         message:
            `Source '${sourceName}' cannot be materialized (colocated ` +
            `'#@ persist'): it is protected by an #(authorize) gate (its own or ` +
            `a joined source's). An authorize expression is evaluated per ` +
            `request; a materialized-once table served frozen carries no gate, ` +
            `so it would be served to everyone, bypassing authorization. This ` +
            `is refused for safety. Drop '#@ persist' from this source, or move ` +
            `the gate to a source that is not materialized.`,
      });
   }
}

/**
 * Names of the source's parameters that are declared but not bound to a value.
 * A Malloy `Parameter` carries `value: ConstantExpr | null`; `null` is an
 * unbound (free) parameter — bound-to-constant parameters have a non-null value.
 *
 * Fail-closed like the given/authorize walks (the caller turns a throw into a
 * refusal): a parameter counts as unbound unless demonstrably bound, so a
 * compiler shift to `value: undefined` can't let a template through. A MISSING
 * `parameters` field is not an error — most sources declare none — so a renamed
 * or relocated field is the one drift this can't catch; the real-compiler
 * eligibility spec is what pins that shape.
 */
function unboundParameterNames(persistSource: PersistSource): string[] {
   const def = persistSource._sourceDef as unknown;
   if (def === null || typeof def !== "object") {
      throw new Error("compiled source definition is not readable");
   }
   const parameters = (def as { parameters?: unknown }).parameters;
   if (parameters === undefined || parameters === null) return [];
   if (typeof parameters !== "object" || Array.isArray(parameters)) {
      throw new Error(
         `compiled source 'parameters' has an unexpected shape (${
            Array.isArray(parameters) ? "array" : typeof parameters
         })`,
      );
   }
   const unbound: string[] = [];
   for (const [name, param] of Object.entries(
      parameters as Record<string, unknown>,
   )) {
      const bound =
         param !== null &&
         typeof param === "object" &&
         (param as { value?: unknown }).value !== null &&
         (param as { value?: unknown }).value !== undefined;
      if (!bound) unbound.push(name);
   }
   return unbound;
}

/**
 * Whether the compiled source (transitively) references any given. Fail-closed:
 * walks the compiled source definition for both of Malloy's given signals —
 * a non-empty `refSummary.givenUsage` (populated on filters and expressions by
 * the reference-tracking walker) and any `given` / `givenReference` IR node —
 * anywhere in the source's filter list, field expressions, or nested pipeline.
 *
 * A generic bounded walk (rather than reading a single well-known field) is
 * deliberate: a given can hide in a field expression or a nested view, not just
 * a top-level `where:`, and missing one is a tenant-isolation breach. The walk
 * is depth- and visited-bounded so a cyclic IR graph cannot hang it, and any
 * introspection failure is treated as "references a given" (fail closed).
 */
function referencesGiven(persistSource: PersistSource): boolean {
   try {
      return walkForGiven(persistSource._sourceDef, new WeakSet(), 0);
   } catch {
      // Fail closed: if we cannot prove the source is given-free, refuse it.
      return true;
   }
}

/** Max IR depth to walk; deep enough for real sources, a hang backstop. */
const MAX_GIVEN_WALK_DEPTH = 200;

function walkForGiven(
   node: unknown,
   seen: WeakSet<object>,
   depth: number,
): boolean {
   if (depth > MAX_GIVEN_WALK_DEPTH) {
      // Refuse rather than risk an unbounded structure hiding a given.
      throw new Error("given-usage walk exceeded max depth");
   }
   if (node === null || typeof node !== "object") return false;
   if (seen.has(node as object)) return false;
   seen.add(node as object);

   if (Array.isArray(node)) {
      for (const item of node) {
         if (walkForGiven(item, seen, depth + 1)) return true;
      }
      return false;
   }

   const record = node as Record<string, unknown>;

   // A given declaration or reference IR node.
   const nodeKind = record.node;
   if (nodeKind === "given" || nodeKind === "givenReference") return true;
   if (record.givenRef !== undefined) return true;

   // Reference-tracking summary: a non-empty givenUsage means this fragment
   // reads a given (the precise, per-fragment signal). It appears either nested
   // under refSummary (per-fragment) OR as a bare `givenUsage` on a query/struct
   // node — treat a non-empty array in EITHER shape as a hit, so the walk stays
   // correct if a future compiler keeps only the summary and prunes the embedded
   // `node:'given'` leaves.
   const refSummary = record.refSummary as
      | { givenUsage?: unknown[] }
      | undefined;
   if (refSummary?.givenUsage && refSummary.givenUsage.length > 0) return true;
   if (Array.isArray(record.givenUsage) && record.givenUsage.length > 0) {
      return true;
   }

   for (const value of Object.values(record)) {
      if (walkForGiven(value, seen, depth + 1)) return true;
   }
   return false;
}

/**
 * Whether the compiled source (transitively) carries an `#(authorize)` gate —
 * on the source itself or on any source reachable through a join. Fail-closed:
 * walks the compiled source definition for annotation notes (`blockNotes` /
 * `notes`) whose text is an authorize annotation. A join embeds the joined
 * SourceDef (with its own blockNotes), so a gate reached through a PLAIN join is
 * found, as is one filed under `annotations.inherits`. An ANNOTATED join is the
 * hole — Malloy replaces the joined struct's annotations outright, so there is no
 * authorize byte left in the subtree to find and no `inherits` to follow; see the
 * join-reach note in this file's header. Any introspection or parse failure is
 * treated as "carries a gate" (fail closed).
 *
 * Deliberately separate from `Model.collectEntryPointGates`, and the two now
 * answer questions with different SHAPES, not just different return types. That
 * one collects the gates to EVALUATE for one request, and follows identity edges
 * only (own annotations, the `inherits`/registry chain, a query-source's
 * derivation base) — it does not trace joins, because a gate states who may query
 * the source it is declared on. This one asks whether ANY gate exists anywhere
 * beneath the source at build time — reaching further than the entry point,
 * because the output is a frozen table rather than a per-request evaluation. They
 * are not two spellings of one rule and should not be merged; see the join-reach
 * note in this file's header for what this one does and does not actually cover.
 */
function referencesAuthorize(persistSource: PersistSource): boolean {
   try {
      return walkForAuthorize(persistSource._sourceDef, new WeakSet(), 0);
   } catch {
      // Fail closed: if we cannot prove the source is authorize-free, refuse it.
      return true;
   }
}

/** True if an annotation string is an `#(authorize)`/`##(authorize)` gate. */
function isAuthorizeAnnotation(text: string): boolean {
   try {
      return parseAuthorizeAnnotation(text) !== null;
   } catch {
      // A malformed authorize annotation still means the author intended a gate.
      return true;
   }
}

function walkForAuthorize(
   node: unknown,
   seen: WeakSet<object>,
   depth: number,
): boolean {
   if (depth > MAX_GIVEN_WALK_DEPTH) {
      throw new Error("authorize-usage walk exceeded max depth");
   }
   if (node === null || typeof node !== "object") return false;
   if (seen.has(node as object)) return false;
   seen.add(node as object);

   if (Array.isArray(node)) {
      for (const item of node) {
         if (walkForAuthorize(item, seen, depth + 1)) return true;
      }
      return false;
   }

   const record = node as Record<string, unknown>;

   // Annotation notes live under `blockNotes` or `notes` — both are per-item
   // slots at the same level, keyed by the author's syntax rather than by scope
   // (see `ownLevelNoteTexts`), so both have to be read. Each entry is either a
   // bare string or a `{ text }` object.
   for (const key of ["blockNotes", "notes"]) {
      const arr = record[key];
      if (!Array.isArray(arr)) continue;
      for (const n of arr) {
         const text =
            typeof n === "string"
               ? n
               : n &&
                   typeof n === "object" &&
                   typeof (n as { text?: unknown }).text === "string"
                 ? (n as { text: string }).text
                 : undefined;
         if (text !== undefined && isAuthorizeAnnotation(text)) return true;
      }
   }

   for (const value of Object.values(record)) {
      if (walkForAuthorize(value, seen, depth + 1)) return true;
   }
   return false;
}
