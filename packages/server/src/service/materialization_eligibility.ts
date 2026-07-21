import type { PersistSource } from "@malloydata/malloy";
import { MaterializationEligibilityError } from "../errors";
import { recordEligibilityRefused } from "../materialization_metrics";

/**
 * Compile-time eligibility gate for materializing a persist source into a
 * *storage* destination (a DuckDB/DuckLake tier table), as opposed to the
 * default in-warehouse path (`storage=source`). This is a HARD REFUSE: an
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
 *
 * The third eligibility property from the design — the served source must
 * compile in DuckDB (portability) — is enforced at *build* time against the
 * captured table schema (see the DuckDB-compile gate in the build path), not
 * here: it needs the post-build authoritative schema this compile-time pass
 * does not have. This pass covers only the two properties determinable from the
 * compiled source alone, so it can fail fast before any warehouse work.
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

   const unbound = unboundParameterNames(persistSource);
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
}

/**
 * Names of the source's parameters that are declared but not bound to a value.
 * A Malloy `Parameter` carries `value: ConstantExpr | null`; `null` is an
 * unbound (free) parameter — bound-to-constant parameters have a non-null value.
 */
function unboundParameterNames(persistSource: PersistSource): string[] {
   const def = persistSource._sourceDef as {
      parameters?: Record<string, { value: unknown } | undefined>;
   };
   const parameters = def.parameters;
   if (!parameters) return [];
   const unbound: string[] = [];
   for (const [name, param] of Object.entries(parameters)) {
      if (param && param.value === null) unbound.push(name);
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
   // reads a given (the precise, per-fragment signal).
   const refSummary = record.refSummary as
      | { givenUsage?: unknown[] }
      | undefined;
   if (refSummary?.givenUsage && refSummary.givenUsage.length > 0) return true;

   for (const value of Object.values(record)) {
      if (walkForGiven(value, seen, depth + 1)) return true;
   }
   return false;
}
