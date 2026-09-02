// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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

import { Annotations } from "@malloydata/malloy";
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
   /**
    * The namespace the rollup's table is created in: this grain's own
    * `#@ preaggregate namespace=`, else everything before the last dot of the
    * base's `#@ persist name=`, else undefined when neither names one.
    *
    * A rollup of X belongs where X lives, unless its author said otherwise. It
    * also decides whether the rollup can be built at all on a dialect that
    * requires qualification: BigQuery rejects an unqualified CREATE, so a bare
    * name is not a cosmetic difference there.
    */
   namespace?: string;
   /**
    * The storage destination this rollup is built into and served from: this
    * grain's own `#@ preaggregate storage=`, else the base's sibling
    * `#@ persist storage=`, else undefined for a colocated rollup built into the
    * base's own warehouse.
    *
    * Mutually exclusive with {@link namespace} in practice: placement inside a
    * destination is derived rather than authored, so the combination is refused
    * at publish (`namespace_with_storage`). {@link emitRollup} enforces the same
    * precedence anyway, so a plan that somehow carried both cannot emit a
    * destination-qualified name the destination has no schema for.
    */
   storage?: string;
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

/**
 * One segment of a namespace: a plain identifier, plus the hyphen.
 *
 * The house rule for a bare-spliced identifier is {@link assertSafeSqlIdentifier}'s
 * `[A-Za-z_][A-Za-z0-9_$]*`, and this is that with one addition. Its own reasoning
 * is why: hyphens were left out because Snowflake, Trino and Unity Catalog all need
 * a hyphenated name quoted, "and BigQuery, the one dialect whose names really do
 * carry hyphens, never reaches this function". A rollup namespace DOES reach
 * BigQuery, where a hyphenated project id is ordinary, so excluding it would refuse
 * `my-project.analytics` — a name the dialect this feature broke on requires.
 *
 * Everything else is out: a space or a quote cannot be spliced bare, and a name
 * needing quotes cannot be joined to a generated table name (see
 * {@link persistNamespace}).
 */
const NAMESPACE_SEGMENT = /^[A-Za-z_][A-Za-z0-9_$-]*$/;

/**
 * Whether every dot-separated segment can be spliced into a generated name.
 *
 * Dots are the one separator that survives: BigQuery addresses a dataset as
 * `project.dataset`, so a namespace is a path, not a single identifier.
 *
 * An empty string is refused by the per-segment test, which `""` fails.
 */
export function isSpliceableNamespace(namespace: string): boolean {
   return namespace.split(".").every((s) => NAMESPACE_SEGMENT.test(s));
}

/** The alias the author's base source is imported under. */
export function baseAlias(baseSourceName: string): string {
   return `${baseSourceName}${BASE_ALIAS_SUFFIX}`;
}

/**
 * The namespace half of a `#@ persist name=` — everything before the LAST dot.
 *
 * `analytics.orders` yields `analytics`; `proj.analytics.orders` yields
 * `proj.analytics`; a bare `orders` yields undefined. Split on the last dot
 * rather than the first because BigQuery names can carry a project as well as a
 * dataset, and the rollup belongs beside the base in whichever of those it names.
 *
 * **A quoted name yields nothing, deliberately.** Splitting one on a dot is not
 * sound — `"My.Schema"` is a single identifier containing a dot, and the last-dot
 * rule tears it into `"My`. Even a well-formed `"A"."B"` would give a quoted
 * prefix that this then joins to an unquoted derived segment, and the two sides of
 * the bind disagree about a mixed path: `quoteManifestTablePath` passes anything
 * already carrying a quote through untouched, while the CREATE side quotes every
 * segment. That disagreement is a known defect for authored names
 * (`quoted-persist-name-colocated`); a derived name must not extend it. Such an
 * author names the rollup's namespace explicitly instead.
 *
 * A trailing dot yields nothing either: the base's own table segment is empty, so
 * the name is malformed and inventing a namespace from it would hide that.
 */
export function persistNamespace(
   persistName: string | undefined,
): string | undefined {
   if (!persistName) return undefined;
   const lastDot = persistName.lastIndexOf(".");
   if (lastDot <= 0) return undefined;
   if (persistName.slice(lastDot + 1).trim() === "") return undefined;
   const candidate = persistName.slice(0, lastDot);
   return isSpliceableNamespace(candidate) ? candidate : undefined;
}

/**
 * Order two rollups on one base so the COARSER is offered first.
 *
 * The composite resolver takes the first member that covers a query, so member
 * order decides which of several covering rollups answers it. Ordering by
 * generated name — which is what this did — resolved that by a grain digest,
 * which is to say arbitrarily: with `grain="b"` and `grain="a, b"`, a query
 * grouping by `b` alone read the `a, b` table because `a_b` sorts first, even
 * though the `b` table is the smaller read and covers it exactly.
 *
 * Fewer grain dimensions therefore win, and that single rule is enough. It might
 * look as though a strict-subset test is also needed — a subset is provably
 * coarser, where a dimension count is only a proxy for one — but subset is
 * SUBSUMED by it: a strict subset always has fewer elements than its superset,
 * so the two rules can never disagree, and the count also orders grains that are
 * not comparable at all (`{a}` before `{b, c}`), which subset alone leaves
 * undecided. The weaker rule would be dead weight beside the stronger one.
 *
 * A dimension count is still only a proxy for cardinality — three tiny
 * dimensions can product out smaller than one large one. Ordering on the
 * manifest's `rowCount` would be the accurate version and is deliberately NOT
 * used: member order would then depend on the bound manifest, so the build leg
 * and the serve leg would synthesize different text and it would change on every
 * refresh. Both legs producing byte-identical text is the property the whole
 * mechanism rests on (see this module's header), and it is not worth narrowing
 * for a sharper proxy.
 *
 * The generated name remains the final tie-break, so the order stays total and
 * deterministic for grains of equal breadth.
 */
export function compareRollupBreadth(a: RollupPlan, b: RollupPlan): number {
   return (
      a.grainDimensions.length - b.grainDimensions.length ||
      a.rollupSourceName.localeCompare(b.rollupSourceName)
   );
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
/**
 * The namespace a rollup inherits from its base's `#@ persist name=`, or undefined
 * when there is none to inherit.
 *
 * Read from the source's annotations rather than from a build plan: synthesis runs
 * before one exists, and the rollup's text has to carry the name it will be built
 * under. Unreadable annotations are treated as absent for the same reason
 * {@link readPreaggregateAnnotation} does — a malformed line must not take the
 * package down.
 *
 * **A `storage=` base lends its DESTINATION, and no namespace.** A rollup of X
 * follows X to the store: the base's rows live there, so the rollup of them
 * belongs there too, and a rollup left behind in the warehouse would be built by
 * reading across the boundary the tier exists to avoid crossing.
 *
 * The namespace is dropped rather than carried because the base's `name=` is a
 * name in the DESTINATION's catalog and placement inside a destination is
 * derived, not authored: a freshly provisioned catalog has no schema, and the
 * build emits a bare `CREATE OR REPLACE TABLE` with no `CREATE SCHEMA`. Carrying
 * it would aim the CREATE at a schema that need not exist. Declaring both is
 * refused outright at publish (`namespace_with_storage`); this is the inherited
 * case of the same rule, where there is nothing to refuse because the author
 * wrote only one of them.
 */
export function basePersistPlacement(source: ValidatableSource): {
   namespace?: string;
   storage?: string;
} {
   if (!source.annotations) return {};
   try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const tag = new Annotations(source.annotations as any).parseAsTag(
         "@",
      ).tag;
      if (!tag.has("persist")) return {};
      // `#@ persist name="x"` parses as two SIBLING keys, not a nested one — the
      // same shape `readPreaggregateAnnotation` handles for `grain`. Nested form
      // first so a future nested spelling wins, sibling as the documented fallback.
      const storage = tag.text("persist", "storage") ?? tag.text("storage");
      if (storage !== undefined && storage.trim() !== "") {
         return { storage: storage.trim() };
      }
      return {
         namespace: persistNamespace(
            tag.text("persist", "name") ?? tag.text("name"),
         ),
      };
   } catch {
      return {};
   }
}

export function planSourcePreaggregation(
   baseSourceName: string,
   source: ValidatableSource,
): RollupPlan[] {
   const { namespace: inheritedNamespace, storage: inheritedStorage } =
      basePersistPlacement(source);
   // Canonical grain -> the measures declared at it. Keyed on the sorted grain so
   // two authors writing the same dimensions in either order land in one entry.
   const byGrain = new Map<
      string,
      {
         grainDimensions: string[];
         measures: RollupMeasure[];
         namespace?: string;
         storage?: string;
      }
   >();

   for (const field of source.fields ?? []) {
      const declaration = readPreaggregateAnnotation(
         field as AnnotatableMeasure,
      );
      if (!declaration.declared || declaration.errors.length > 0) continue;

      // Hidden measures never reach a rollup. Refused at publish
      // (`non_public_measure`), and skipped here for the same reason every other
      // refusal is: the planner and the validator must not disagree about what is
      // buildable. This one is also the last line of defence — a rollup's stored
      // table is served without the source's field visibility applying to it.
      if (
         (field as { accessModifier?: unknown }).accessModifier != null &&
         (field as { accessModifier?: unknown }).accessModifier !== "public"
      ) {
         continue;
      }

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
         // First one named wins, in the field order the IR reports. Safe to be
         // arbitrary only because two measures at ONE grain naming different
         // namespaces is refused at publish (`conflicting_namespace`): the grain is
         // a single table, so it cannot honour both. Across grains there is no
         // conflict to resolve — each entry carries its own.
         entry.namespace ??= grain.namespace;
         // Same rule and the same justification as `namespace` above: two
         // measures at ONE grain naming different destinations is refused at
         // publish (`conflicting_storage`), because the grain is a single table
         // and cannot be built in two stores.
         entry.storage ??= grain.storage;
         byGrain.set(key, entry);
      }
   }

   return [...byGrain.values()]
      .map(({ grainDimensions, measures, namespace, storage }) => ({
         baseSourceName,
         rollupSourceName: rollupSourceName(baseSourceName, grainDimensions),
         // Author's choice first, the base's namespace as the fallback: a rollup of
         // X belongs where X lives unless its author said otherwise.
         namespace: namespace ?? inheritedNamespace,
         // Same precedence, same reason: a rollup of X follows X into the store
         // unless its author placed this grain somewhere else.
         storage: storage ?? inheritedStorage,
         grainDimensions,
         // Sorted so the emitted text does not depend on field order in the IR.
         measures: [...measures].sort((a, b) => a.name.localeCompare(b.name)),
      }))
      .sort(compareRollupBreadth);
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
   // A destination wins, and takes NO `name=`. Placement inside a destination is
   // derived rather than authored: the resolver refuses a dotted `name=` outright
   // because a freshly provisioned catalog has no schema and the build emits a
   // bare `CREATE OR REPLACE TABLE`. Emitting one here would produce a refusal
   // naming a generated source and a `name=` the author never wrote.
   //
   // Bare is safe: the build self-assigns the physical name from the source name
   // (`selfAssignTableName`), identically for both tiers, and a rollup's source
   // name carries the grain digest — so it is unique per grain without a `name=`
   // to make it so.
   //
   // Otherwise the colocated rule, unchanged: named only when a namespace was
   // named or inherited, since the build self-assigns from the source name, which
   // is what every dialect but BigQuery accepts, and inventing one would be a
   // guess.
   const persist = plan.storage
      ? `#@ persist storage=${plan.storage}`
      : plan.namespace
        ? `#@ persist name="${plan.namespace}.${plan.rollupSourceName}"`
        : "#@ persist";
   return `${persist}
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
