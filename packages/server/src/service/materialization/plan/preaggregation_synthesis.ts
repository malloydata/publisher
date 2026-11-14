// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The v1 `#@ preaggregate` planner: turn validated declarations into rollup
 * plans, one per distinct grain, which the v1 cutover reads into the rollup
 * leg as `#(preaggregate)` plans (G4) and the build plan builds.
 *
 * The composite text it once emitted (an aliased import of the base, the
 * rollup members and a `compose()` re-exposing the base's name) retired with
 * the cutover (6g).
 *
 * ## Determinism is a correctness property
 *
 * A rollup's name and table derive from its plan, so grain dimensions,
 * measures and plans are sorted, and the name derives from the canonical
 * grain: two plannings of one model must name the same tables.
 *
 * Pure: takes compiled IR, returns plans, touches no I/O.
 */

import { Annotations } from "@malloydata/malloy";
import { parseRouteTag } from "../../motly";
import { createHash } from "node:crypto";
import {
   readPreaggregateAnnotation,
   type AnnotatableMeasure,
} from "./preaggregation_annotation";
import {
   classifyMeasureAdditivity,
   type ReaggregateFunction,
} from "./preaggregation_classifier";
import { isSpliceableNamespace } from "./preaggregation_namespace";
import { fieldNotes } from "../text/field_notes";
import type { ValidatableSource } from "./preaggregation_validation";

/** Suffix on the stored partial-aggregate column for a measure. */
const PARTIAL_SUFFIX = "__partial";

/** One measure served by a rollup, with the merge to apply to its partial. */
interface RollupMeasure {
   /** The measure's name on the base, and on the rollup member. */
   name: string;
   /** The stored column holding the partial aggregate. */
   partialName: string;
   /** The aggregate applied to `partialName` to merge it. */
   reaggregate: ReaggregateFunction;
   /**
    * The measure's annotation lines (`#(classification)`, `#(doc)`, a render
    * tag such as `# currency`), written again on the merged measure so a
    * rollup's result column carries what live's does. `#@` directives are
    * left out: they declare builds, not the column.
    */
   notes?: string[];
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
    * A rollup of X belongs where X lives unless its author said otherwise. Not
    * cosmetic: BigQuery rejects an unqualified CREATE.
    */
   namespace?: string;
   /**
    * The storage destination this rollup is built into and served from — this
    * grain's own `#@ preaggregate storage=`, and only ever that. Undefined for a
    * colocated rollup built into the base's own warehouse.
    *
    * NOT inherited from the base's `#@ persist storage=`; see
    * {@link basePersistNamespace} for why a destination cannot usefully be.
    *
    * Mutually exclusive with {@link namespace} (refused at publish as
    * `namespace_with_storage`).
    */
   storage?: string;
   /** The one grain, canonically sorted. */
   grainDimensions: string[];
   /** The measures served here, sorted by name. */
   measures: RollupMeasure[];
   /**
    * The base's Malloy `sourceID`, `<declared name>@<declaring file URL>`. A
    * model that only imports the base (an `index.malloy` re-export) cannot
    * rename-import it from itself, so the companion imports it from the file
    * that declares it (#6646). Absent when the compiler gave none.
    */
   baseSourceID?: string;
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
   // Dots are folded out so the name, which IS the rollup's self-assigned
   // physical table name, stays one unqualified identifier; a consumer reading
   // a dotted name as schema-qualified would address the wrong thing. Dotted
   // grains are refused at publish anyway; this makes it a property of the
   // name. The digest is over the RAW dimensions, so `a.b` and `a_b` differ.
   const slug = grainDimensions
      .join("_")
      .replace(/\./g, "_")
      .slice(0, NAME_SLUG_LIMIT);
   return `${baseSourceName}__preagg__${slug}__${grainDigest(grainDimensions)}`;
}

/**
 * The namespace half of a `#@ persist name=` — everything before the LAST dot.
 *
 * `analytics.orders` yields `analytics`; `proj.analytics.orders` yields
 * `proj.analytics`; a bare `orders` yields undefined. Split on the last dot
 * rather than the first because BigQuery names can carry a project as well as a
 * dataset, and the rollup belongs beside the base in whichever of those it names.
 *
 * **A quoted name yields nothing, deliberately.** `"My.Schema"` is one
 * identifier the last-dot rule would tear apart, and even `"A"."B"` would join a
 * quoted prefix to an unquoted segment, which the two sides of the bind quote
 * differently (`quoteManifestTablePath` passes quoted paths through; the CREATE
 * side quotes every segment). Such an author names the namespace explicitly.
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
 * order decides which covering rollup answers it. Fewer grain dimensions win
 * (which subsumes a strict-subset rule, and also orders incomparable grains).
 *
 * A dimension count is only a proxy for size. Ordering on the manifest's
 * `rowCount` is deliberately NOT used: member order would then depend on the
 * bound manifest, and the build and serve legs would synthesize different text
 * (see this module's header).
 *
 * The generated name is the final tie-break, so the order is total.
 */
export function compareRollupBreadth(a: RollupPlan, b: RollupPlan): number {
   return (
      a.grainDimensions.length - b.grainDimensions.length ||
      a.rollupSourceName.localeCompare(b.rollupSourceName)
   );
}

/**
 * The namespace a rollup inherits from its base's `#@ persist name=`, or undefined
 * when there is none to inherit.
 *
 * Read from annotations because synthesis runs before a build plan exists.
 * Unreadable annotations are treated as absent, as in
 * {@link readPreaggregateAnnotation}.
 *
 * **A `storage=` base lends nothing — not its destination, and not a namespace.**
 *
 * Not the DESTINATION, because inheritance requires `#@ persist` on the base and
 * both cases fail:
 *  - **Query-shaped base**: it builds its own stored table, whose binding claims
 *    the author name the rollups need, so they are dropped from the serve shape
 *    — built, refreshed, and unreadable.
 *  - **Table extended with measures** (the usual case): the annotation parses,
 *    but the build refuses the whole run because the `#@ persist` source was
 *    dropped from the plan (`relevantDropped` in materialization_service.ts).
 * The destination is therefore written on the `#@ preaggregate` line. (The
 * colocated tier composes members synthesis names, with the base under an
 * import alias, so nothing keys on the author's name; that is why a `name=`
 * namespace inherits usefully.)
 *
 * Not the NAMESPACE, because the base's `name=` is a name in the destination's
 * catalog and means nothing in the source warehouse where the rollup is built.
 */
export function basePersistNamespace(
   source: ValidatableSource,
): string | undefined {
   if (!source.annotations) return undefined;
   try {
      const tag = parseRouteTag(
         // eslint-disable-next-line @typescript-eslint/no-explicit-any
         new Annotations(source.annotations as any),
         "@",
      ).tag;
      if (!tag.has("persist")) return undefined;
      // `#@ persist name="x"` parses as SIBLING keys; nested form first, sibling
      // as the documented fallback (as `readPreaggregateAnnotation` does).
      const storage = tag.text("persist", "storage") ?? tag.text("storage");
      if (storage !== undefined && storage.trim() !== "") return undefined;
      return persistNamespace(tag.text("persist", "name") ?? tag.text("name"));
   } catch {
      return undefined;
   }
}

/**
 * A field the source does not publicly expose.
 *
 * Mirrors `isAccessRestricted` in materialization_serve_transform.ts and
 * preaggregation_validation.ts, and fails CLOSED: anything not exactly `public`
 * is hidden, so a future modifier kind is refused by default. No modifier at
 * all is public.
 */
function isHidden(field: { accessModifier?: unknown } | undefined): boolean {
   return field?.accessModifier != null && field.accessModifier !== "public";
}

/**
 * Group one source's `#@ preaggregate` declarations into rollups, one per
 * distinct grain (ten measures at one grain are one table and one `GROUP BY`).
 *
 * Assumes the source passed {@link validateSourcePreaggregation}: a declaration
 * that would be refused is skipped here, so the two agree on what is buildable.
 */
export function planSourcePreaggregation(
   baseSourceName: string,
   source: ValidatableSource,
): RollupPlan[] {
   const inheritedNamespace = basePersistNamespace(source);
   /** Resolve a grain dimension back to the field it names, if it names one. */
   const fieldNamed = (name: string) =>
      (source.fields ?? []).find((f) => (f.as ?? f.name) === name);
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

      // Hidden measures (and hidden grain dimensions, below) never reach a
      // rollup. This skip IS the access control; the publish refusal is only
      // the message. It guarantees no table is built and nothing is served.
      if (isHidden(field)) continue;

      const additivity = classifyMeasureAdditivity(field as never);
      if (!additivity.additive) continue;

      // Several grains on one measure are several rollups (see
      // preaggregation_annotation.ts for why).
      for (const grain of declaration.grains) {
         const grainDimensions = grain.dimensions;
         if (grainDimensions.length === 0) continue;
         // A grain is STORED, so a hidden dimension in it would be reachable
         // through the rollup. Checked per grain, since only one of a measure's
         // grains may name a hidden field. A dimension resolving to no field is
         // not hidden; the validator refuses it as unknown.
         if (grainDimensions.some((d) => isHidden(fieldNamed(d)))) continue;

         const key = grainDimensions.join("\u0000");
         const entry = byGrain.get(key) ?? { grainDimensions, measures: [] };
         const name = field.as ?? field.name;
         entry.measures.push({
            name,
            partialName: `${name}${PARTIAL_SUFFIX}`,
            reaggregate: additivity.reaggregate,
            notes: fieldNotes(field).filter((n) => !n.startsWith("#@")),
         });
         // First one named wins. Safe only because differing values at ONE
         // grain are refused at publish (`conflicting_namespace`,
         // `conflicting_storage`).
         entry.namespace ??= grain.namespace;
         entry.storage ??= grain.storage;
         byGrain.set(key, entry);
      }
   }

   return [...byGrain.values()]
      .map(({ grainDimensions, measures, namespace, storage }) => ({
         baseSourceName,
         rollupSourceName: rollupSourceName(baseSourceName, grainDimensions),
         namespace: namespace ?? inheritedNamespace,
         // Never inherited from the base (see basePersistNamespace).
         storage,
         grainDimensions,
         // Sorted so the emitted text does not depend on field order in the IR.
         measures: [...measures].sort((a, b) => a.name.localeCompare(b.name)),
      }))
      .sort(compareRollupBreadth);
}
