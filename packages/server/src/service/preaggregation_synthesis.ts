/**
 * Synthesis: turn validated `#@ preaggregate` declarations into a rollup plan and
 * the Malloy text that implements it (handoff Work item 3).
 *
 * ## The shape of the output, and why it is a separate model
 *
 * The synthesized text does not edit the author's model. It is a NEW model that
 * imports the base under an alias, rolls it up, and re-exposes the original name
 * as a composite:
 *
 * ```malloy
 * import { orders__preagg_base is orders } from "orders.malloy"
 *
 * #@ persist
 * source: orders__preagg__category__1a2b3c4d is orders__preagg_base -> {
 *   group_by: category
 *   aggregate: total__partial is total
 * } extend {
 *   measure: total is total__partial.sum()
 * }
 *
 * source: orders is compose(orders__preagg__category__1a2b3c4d, orders__preagg_base)
 * ```
 *
 * Three things fall out of that and each one is load-bearing:
 *
 *  - **No text surgery.** Making `orders` a composite requires the original under
 *    another name, and `import { new is old }` provides exactly that, so nothing
 *    parses or rewrites the author's source. Rewriting it would be real surgery —
 *    the risk this approach exists to avoid — and with rename-on-import there is
 *    no surgery to get wrong.
 *  - **No discovery leak.** The author's model is untouched, so the model the API
 *    introspects still exports `orders` alone. The rollup exists only in this
 *    second model, which is compiled for the build plan and for the transient
 *    serve model and is never the discoverable one.
 *  - **The base's own measures are referenced BY NAME** (`total__partial is
 *    total`), never re-printed from IR. So a measure that aggregates through a
 *    join needs no special handling — the join comes along with the base — and
 *    there is no expression printer to disagree with the compiler.
 *
 * ## Determinism is a correctness property here, not tidiness
 *
 * The build leg and the serve leg each synthesize (that is the seam decision), so
 * both must produce byte-identical text: the rollup's `sourceEntityId` is derived
 * from its compiled definition, and if the two legs disagree the serve model binds
 * to a table the build leg never created and every query silently falls back to
 * live. Hence: grain dimensions sorted, measures sorted, plans sorted, and the
 * name derived from the canonical grain rather than from authoring order.
 *
 * Pure: takes compiled IR, returns a plan and a string, touches no I/O.
 */

import { createHash } from "node:crypto";
import {
   readPreaggregateAnnotation,
   type AnnotatableMeasure,
} from "./preaggregation_annotation";
import {
   classifyMeasureAdditivity,
   type ReaggregateFunction,
} from "./preaggregation_classifier";
import type { ValidatableSource } from "./preaggregation_validation";

/** The alias the base is imported under, and the last compose() member. */
export const BASE_ALIAS_SUFFIX = "__preagg_base";

/** Suffix on the stored partial-aggregate column for a measure. */
export const PARTIAL_SUFFIX = "__partial";

/** One measure served by a rollup, with the merge to apply to its partial. */
export interface RollupMeasure {
   /** The measure's name on the base, and on the rollup member. */
   name: string;
   /** The stored column holding the partial aggregate. */
   partialName: string;
   /** The aggregate applied to `partialName` to merge it. */
   reaggregate: ReaggregateFunction;
}

/** One synthesized rollup: one base, one grain, one table, one `GROUP BY`. */
export interface RollupPlan {
   /** The source whose measures were annotated. Queries name this. */
   baseSourceName: string;
   /** The synthesized `#@ persist` source's name. Deterministic. */
   rollupSourceName: string;
   /** The one grain, canonically sorted. */
   grainDimensions: string[];
   /** The measures served here, sorted by name. */
   measures: RollupMeasure[];
}

/**
 * A short digest of the canonical grain, appended to the rollup's name.
 *
 * Needed because the readable part is lossy: a grain of `[a_b, c]` and one of
 * `[a, b_c]` both slug to `a_b_c`, and two rollups sharing a name would collapse
 * into one table serving the wrong queries. The digest is over the joined grain
 * with a separator that cannot appear in a Malloy identifier, so it distinguishes
 * them.
 */
function grainDigest(grainDimensions: string[]): string {
   return createHash("sha256")
      .update(grainDimensions.join("\u0000"))
      .digest("hex")
      .slice(0, 8);
}

/** How much of the grain goes in the readable part of the name. */
const NAME_SLUG_LIMIT = 40;

/**
 * The rollup's source name: readable enough to recognize in a build plan, a
 * manifest and a log line, and unique by digest.
 */
export function rollupSourceName(
   baseSourceName: string,
   grainDimensions: string[],
): string {
   const slug = grainDimensions.join("_").slice(0, NAME_SLUG_LIMIT);
   return `${baseSourceName}__preagg__${slug}__${grainDigest(grainDimensions)}`;
}

/** The alias the author's base source is imported under. */
export function baseAlias(baseSourceName: string): string {
   return `${baseSourceName}${BASE_ALIAS_SUFFIX}`;
}

/**
 * Group one source's `#@ preaggregate` declarations into rollups — one per
 * distinct grain, because ten measures at one grain should be one table and one
 * `GROUP BY`, not ten.
 *
 * Assumes the source has already passed {@link validateSourcePreaggregation}: a
 * declaration that would be refused at publish is skipped here rather than
 * re-reported, so the two never disagree about what is buildable.
 */
export function planSourcePreaggregation(
   baseSourceName: string,
   source: ValidatableSource,
): RollupPlan[] {
   // Canonical grain -> the measures declared at it. Keyed on the sorted grain so
   // two authors writing the same dimensions in either order land in one entry.
   const byGrain = new Map<
      string,
      { grainDimensions: string[]; measures: RollupMeasure[] }
   >();

   for (const field of source.fields ?? []) {
      const declaration = readPreaggregateAnnotation(
         field as AnnotatableMeasure,
      );
      if (!declaration.declared || declaration.errors.length > 0) continue;

      const additivity = classifyMeasureAdditivity(field as never);
      if (!additivity.additive) continue;

      // A measure may be declared at several grains, which is several rollups:
      // the finest grain that covers a query is often far larger than a coarser
      // one, so one combined rollup would cover the same queries while saving
      // much less. It joins each grain's group here.
      for (const grain of declaration.grains) {
         const grainDimensions = grain.dimensions;
         if (grainDimensions.length === 0) continue;

         const key = grainDimensions.join("\u0000");
         const entry = byGrain.get(key) ?? { grainDimensions, measures: [] };
         const name = field.as ?? field.name;
         entry.measures.push({
            name,
            partialName: `${name}${PARTIAL_SUFFIX}`,
            reaggregate: additivity.reaggregate,
         });
         byGrain.set(key, entry);
      }
   }

   return [...byGrain.values()]
      .map(({ grainDimensions, measures }) => ({
         baseSourceName,
         rollupSourceName: rollupSourceName(baseSourceName, grainDimensions),
         grainDimensions,
         // Sorted so the emitted text does not depend on field order in the IR.
         measures: [...measures].sort((a, b) => a.name.localeCompare(b.name)),
      }))
      .sort((a, b) => a.rollupSourceName.localeCompare(b.rollupSourceName));
}

/**
 * Plan every source in a compiled model's contents.
 *
 * Sorted by base source name so the plan — and therefore the synthesized text —
 * does not depend on the order the model happened to declare its sources in.
 */
export function planModelPreaggregation(
   contents: Record<string, unknown>,
): RollupPlan[] {
   return Object.entries(contents)
      .sort(([a], [b]) => a.localeCompare(b))
      .flatMap(([name, object]) => {
         if (!object || typeof object !== "object") return [];
         return planSourcePreaggregation(name, object as ValidatableSource);
      });
}

/** One rollup's `#@ persist source: …` declaration. */
function emitRollup(plan: RollupPlan): string {
   const alias = baseAlias(plan.baseSourceName);
   const partials = plan.measures
      .map((m) => `    ${m.partialName} is ${m.name}`)
      .join("\n");
   // The re-declared measure keeps the base's NAME, so a query asking for
   // `total` finds it on the member; only the stored column is renamed.
   const merged = plan.measures
      .map((m) => `    ${m.name} is ${m.partialName}.${m.reaggregate}()`)
      .join("\n");
   return `#@ persist
source: ${plan.rollupSourceName} is ${alias} -> {
  group_by:
${plan.grainDimensions.map((d) => `    ${d}`).join("\n")}
  aggregate:
${partials}
} extend {
  measure:
${merged}
}`;
}

/**
 * The synthesized model text for one author model.
 *
 * `importPath` is what the emitted `import` statement resolves against — the
 * author's model as the compiler will see it from the synthesized model's own
 * URL. Returns `undefined` when nothing was declared, so a caller can skip the
 * extra compile entirely rather than compiling a model that adds nothing.
 *
 * `experimentalFlags` are re-declared because `##!` flags do not cross an import:
 * the synthesized model uses `compose()` and `#@ persist` in its own right.
 */
export function synthesizePreaggregationModel(
   plans: RollupPlan[],
   importPath: string,
   experimentalFlags = "persistence composite_sources",
): string | undefined {
   if (plans.length === 0) return undefined;

   // One import line per base, even when it has several grains.
   const bases = [...new Set(plans.map((p) => p.baseSourceName))].sort();
   const imports = bases
      .map(
         (base) =>
            `import { ${baseAlias(base)} is ${base} } from ${JSON.stringify(importPath)}`,
      )
      .join("\n");

   // The base LAST in every compose(), so it is the fallback member: the resolver
   // takes the first member that covers the query, and the base covers everything.
   const composites = bases
      .map((base) => {
         const members = plans
            .filter((p) => p.baseSourceName === base)
            .map((p) => p.rollupSourceName);
         return `source: ${base} is compose(${[...members, baseAlias(base)].join(", ")})`;
      })
      .join("\n");

   return `##! experimental { ${experimentalFlags} }
${imports}

${plans.map(emitRollup).join("\n\n")}

${composites}
`;
}
