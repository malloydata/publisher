import type { PersistSource } from "@malloydata/malloy";
import { MaterializationEligibilityError } from "../errors";
import { recordEligibilityRefused } from "../materialization_metrics";
import type { AnnotationNote } from "./annotations";
import { parseAuthorizeAnnotation } from "./authorize";
import type { PersistSourceGateOutcome } from "./build_plan";

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
 * packages that build and serve correctly today.
 *
 * Unlike the storage tier, a colocated artifact is NOT served frozen with
 * respect to the gate: the entry point's own `#(authorize)` is re-evaluated on
 * every query, grafted onto the SAME entry point (`Model.buildGraftedMaterializer`
 * / `resolveGraftTarget`) whether that entry point resolves to a live
 * recompute or a same-connection substitution of the materialized table.
 * Persistence changes only where the rows are read FROM, never whether the
 * row filter is appended — so a `gateOutcome` of `{classification: "row_level",
 * attributed: true}` (the entry point's gate is PROVEN to compile to a row
 * filter, and PROVEN to be the only gate reachable beneath the source — see
 * `isAuthorizeAttributedToEntryPoint`) is admitted: the served artifact grants
 * nothing a live query would not, and the only thing lost is freshness (a row
 * whose access decision changed is served under the OLD decision until the
 * next rebuild, since the build itself never evaluates the gate).
 *
 * Refused with no `gateOutcome`, or one classifying `rejected` or unattributed,
 * for the ORIGINAL reason: this pass alone (`referencesAuthorize`'s deep walk)
 * cannot tell whether the entry point's own gate is even expressible as a row
 * filter, or whether a second gate hides behind a join outside the entry
 * point's own identity chain — either way there is nothing here to prove the
 * artifact matches what a live query enforces, so it fails closed.
 *
 * This gate carries a SECOND job that its own justification above does not
 * mention, and narrowing it on the strength of that justification alone would
 * open a hole. Pre-aggregation (`#@ preaggregate`) synthesizes each rollup as a
 * colocated `#@ persist` over an import of the annotated base, and none of the
 * `preaggregation_*` modules has any authorize awareness of its own — so this
 * refusal is also the only thing standing between an `#(authorize)`-gated source
 * and the pre-aggregation tier. `referencesAuthorize` finds the gate through the
 * import → rename → `query_source` chain, which is why it holds. A rollup
 * GROUPS across the gated column by construction, so the column is not even
 * present to filter on afterwards — the relaxation above therefore never
 * applies to `origin === "preaggregate"`, regardless of `gateOutcome`; that
 * refusal stays unconditional. See `docs/materialization.md`, and the
 * rollup-shaped test in this module's spec.
 *
 * `origin` names the annotation the AUTHOR wrote, so the refusal can be
 * actionable for a source they never typed. A rollup's name is synthesized
 * (`orders__preagg__category__<hash>`) and appears nowhere in their model, and
 * telling them to "drop `#@ persist`" when what they wrote is `#@ preaggregate`
 * sends them looking for a line that does not exist. Callers pass
 * `"preaggregate"` when `CompiledBuildPlan.preaggregatePlans` has an entry for
 * the source — the same signal `build_plan.ts` reports as `origin`.
 *
 * @throws {MaterializationEligibilityError} (HTTP 422) naming the source, the
 *   annotation to remove, and the alternative of moving the gate to a source
 *   that is not materialized.
 */
export function assertColocatedPersistNotAuthorizeGated(
   persistSource: PersistSource,
   sourceName: string = persistSource.name,
   origin: "persist" | "preaggregate" = "persist",
   gateOutcome?: PersistSourceGateOutcome,
): void {
   if (!referencesAuthorize(persistSource)) return;

   if (
      origin === "persist" &&
      gateOutcome?.classification === "row_level" &&
      gateOutcome.attributed
   ) {
      // Proven safe above: the entry point's own gate compiles to a row
      // filter and nothing else is reachable beneath it, so colocated serving
      // grafts exactly what a live query would.
      return;
   }

   // Reuses the "authorize" reason: this is the same underlying refusal
   // (a frozen table serving an authorize-gated relation to everyone) as
   // the storage-destination case, just reached via the colocated path.
   recordEligibilityRefused("authorize");
   const gated =
      origin === "preaggregate"
         ? `the source '${sourceName}' rolls up is protected by an ` +
           `#(authorize) gate (its own or a joined source's)`
         : `it is protected by an #(authorize) gate (its own or a joined ` +
           `source's)`;
   const remedy =
      origin === "preaggregate"
         ? `Remove the '#@ preaggregate' annotation from the gated source's ` +
           `measure(s), or move the gate to a source that is not ` +
           `pre-aggregated.`
         : `Drop '#@ persist' from this source, or move the gate to a source ` +
           `that is not materialized.`;
   const what =
      origin === "preaggregate"
         ? `Pre-aggregation rollup '${sourceName}' cannot be built`
         : `Source '${sourceName}' cannot be materialized (colocated ` +
           `'#@ persist')`;
   // Only true of a rollup: it GROUPS, so the gated column is not even
   // present to filter on afterwards.
   const alsoRollup =
      origin === "preaggregate"
         ? ` A rollup also groups ACROSS the gated column, so it could not ` +
           `be row-filtered afterwards even in principle.`
         : "";
   // A plain `#@ persist` gate that is row-level but not (yet) proven
   // attributed reads the same as an unclassifiable one here: this function
   // has no visibility into WHY `gateOutcome` didn't clear the bar (missing,
   // rejected, or a join-only gate outside its identity chain), so the
   // message stays generic rather than guessing.
   throw new MaterializationEligibilityError({
      message:
         `${what}: ${gated}. An authorize expression is evaluated per ` +
         `request; without a proven row-level, fully-attributed ` +
         `classification this pass cannot show the served artifact matches ` +
         `what a live query would enforce.` +
         `${alsoRollup} This is refused for safety. ${remedy}`,
   });
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

/**
 * True if an annotation note is an authorize gate — by Malloy's own routing, via
 * {@link parseAuthorizeAnnotation}. Sharing the parser's classification is what
 * keeps this refusal in step with enforcement: a spelling the query path gates on
 * but this one does not is a gated source that can be frozen into an artifact and
 * served to everyone. The block form `#|(authorize)` was exactly that gap while
 * the classification was a prefix regex.
 */
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

/**
 * Whether every `#(authorize)` note reachable (transitively, INCLUDING
 * joins) beneath the compiled source is also reachable WITHOUT crossing a
 * join. The no-joins walk here is a superset of, and believed to cover, what
 * `collectEntryPointGates` (`./gate_classification`) actually reaches by
 * following only identity edges (own annotations -> `ancestorGateExprs`'s
 * `extend` chain -> a query-source's derivation base -> its
 * `compositeResolvedSourceDef`) — the two are not proven equivalent, so a gap
 * (a note off every identity edge but still reachable without crossing a
 * join) is not ruled out, only unobserved. `false` means the deep walk found
 * a note reachable ONLY through a join: `referencesAuthorize`'s refusal is
 * real for it today, but a row-level CLASSIFICATION of the entry point's own
 * gate would say nothing about it, so a caller deciding whether to relax the
 * colocated-persist refusal must require this to be `true`, not just a
 * `row_level` classification.
 *
 * Object identity (matching `ownLevelNotes`'s convention, and
 * `findSourceByOwnAnnotationIdentity`'s in `./gate_classification`), not text
 * — two independently authored gates can share text.
 *
 * Fail-closed: any introspection failure is treated as unattributed.
 */
export function isAuthorizeAttributedToEntryPoint(
   persistSource: PersistSource,
): boolean {
   try {
      const deep = new Set<AnnotationNote>();
      walkForAuthorizeNotes(
         persistSource._sourceDef,
         new WeakSet(),
         0,
         deep,
         true,
      );
      const noJoins = new Set<AnnotationNote>();
      walkForAuthorizeNotes(
         persistSource._sourceDef,
         new WeakSet(),
         0,
         noJoins,
         false,
      );
      for (const note of deep) {
         if (!noJoins.has(note)) return false;
      }
      return true;
   } catch {
      return false;
   }
}

/**
 * Deep walk collecting authorize annotation NOTE objects (by identity)
 * rather than {@link walkForAuthorize}'s boolean — a caller needs to know
 * WHICH notes were found, not just whether any were. Its own recursion,
 * independent of `walkForAuthorize`'s (rather than a shared core), so that
 * function's early-return-on-first-match stays untouched by this
 * collect-everything walk.
 *
 * `crossJoins=false` stops at a join field — matched the same way malloy's
 * own `isJoined` does (`'join' in sd`), duck-typed here rather than fighting
 * `TypedDef`'s union to call the exported predicate on a generic IR node
 * (same spirit as `gate_classification.ts`'s own duck-typed casts).
 * `crossJoins=true` reduces to `walkForAuthorize`'s full reach.
 */
function walkForAuthorizeNotes(
   node: unknown,
   seen: WeakSet<object>,
   depth: number,
   found: Set<AnnotationNote>,
   crossJoins: boolean,
): void {
   if (depth > MAX_GIVEN_WALK_DEPTH) {
      throw new Error("authorize-usage walk exceeded max depth");
   }
   if (node === null || typeof node !== "object") return;
   if (seen.has(node as object)) return;
   seen.add(node as object);

   if (Array.isArray(node)) {
      for (const item of node) {
         walkForAuthorizeNotes(item, seen, depth + 1, found, crossJoins);
      }
      return;
   }

   const record = node as Record<string, unknown>;
   if (!crossJoins && "join" in record) return;

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
         // Malloy's Note is always `{text, at}`, never a bare string, but Set
         // identity is value equality for a string — two independently
         // authored gates sharing text would collapse into one entry and
         // could read as attributed for a join-only gate. Guard rather than
         // rely on that never happening.
         if (
            text !== undefined &&
            isAuthorizeAnnotation(text) &&
            typeof n === "object"
         ) {
            found.add(n as AnnotationNote);
         }
      }
   }

   for (const value of Object.values(record)) {
      walkForAuthorizeNotes(value, seen, depth + 1, found, crossJoins);
   }
}
