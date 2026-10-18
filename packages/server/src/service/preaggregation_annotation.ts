// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Reader for the `#@ preaggregate` measure annotation — the authoring surface
 * for pre-aggregation (docs/preaggregation.md).
 *
 * ## A measure may be rolled up at several grains
 *
 * Each `#@ preaggregate` line on a measure is one grain, and each grain becomes
 * its own rollup:
 *
 *   #@ preaggregate grain="category"
 *   #@ preaggregate grain="order_day"
 *   measure: total is amount.sum()
 *
 * Two rollups, not one: a rollup serves any SUBSET of its grain, but a combined
 * grain's size is roughly the product of its dimensions' cardinalities and can
 * approach the base table, while each grain alone is small. Where several
 * rollups cover one query, the one with the fewest grain dimensions is offered
 * first (`compareRollupBreadth`), a proxy for size.
 *
 * ## Why this reads the annotation NOTES and not the merged tag
 *
 * The merged MOTLY tag holds one value per key, so two `grain=` lines collapse
 * to the last with no diagnostic. Grains are therefore collected from the
 * individual notes, with the smallest possible set of ordering rules: **notes
 * are processed in source order; a `#@ preaggregate` line with a grain adds
 * one; a `#@ -preaggregate` line clears every grain accumulated so far.**
 * `namespace=` and `storage=` bind to the grain on their own line and are
 * cleared with it (a grain IS a table, so each can be placed differently).
 *
 * This relies on the IR delivering `blockNotes` in source order, and on an
 * extending source's fields carrying the base's annotations in their own
 * `blockNotes` (so an extend that negates reads as `[declare, negate]`).
 *
 * One deliberate divergence from the merged tag, in the safe direction: for
 *
 *   #@ preaggregate grain="a"
 *   #@ -preaggregate
 *   #@ preaggregate grain="b"
 *
 * the merged tag reports UNDECLARED (sibling keys), while this returns `b`. The
 * divergence can only turn ON something the merged tag lost; a trailing
 * `#@ -preaggregate` still clears everything.
 *
 * ## Strictness
 *
 * A declaration that cannot be used is an error, not a silent skip, so an
 * annotation that does nothing is never possible. Callers turn errors into a
 * 400.
 */

import { Annotations } from "@malloydata/malloy";
import { isSpliceableNamespace } from "./preaggregation_synthesis";
import type { LogMessage } from "@malloydata/malloy";

/** One grain a measure is declared at — one rollup. */
interface PreaggregateGrain {
   /**
    * The grain's dimensions, canonically sorted and de-duplicated.
    *
    * Sorting is not cosmetic: synthesis derives the rollup's name and identity
    * from this list, so two authors who write the same dimensions in different
    * orders must land on ONE table rather than two identical ones.
    */
   dimensions: string[];
   /** The grain exactly as written, for error text and diagnostics. */
   text: string;
   /**
    * The dataset/schema this rollup's table is created in, when named on the
    * same line (`#@ preaggregate grain="category" namespace="analytics"`). The
    * table name itself is always derived. Undefined when unspecified, in which
    * case the base's `#@ persist name=` supplies it.
    */
   namespace?: string;
   /**
    * The storage destination this rollup's table is built into and served from,
    * when the author named one (`#@ preaggregate grain="category"
    * storage=lake`). Undefined means colocated: built into the base's own
    * warehouse.
    *
    * Unlike `namespace`, NOTHING supplies it when unspecified: a destination is
    * never inherited from the base's `#@ persist storage=` (see
    * `basePersistNamespace` for why).
    */
   storage?: string;
}

/** A `#@ preaggregate` line that is present but unusable. */
interface PreaggregateDeclarationError {
   kind:
      | "missing_grain"
      | "empty_grain"
      | "invalid_namespace"
      | "empty_storage"
      | "invalid_storage";
   /** Names the measure and the fix; becomes the body of a publish-time 400. */
   message: string;
}

/** What `#@ preaggregate` says about one measure. */
interface PreaggregateDeclaration {
   /**
    * True when at least one `#@ preaggregate` line is in effect for this measure,
    * with negation applied.
    */
   declared: boolean;
   /**
    * Every grain in effect, one per rollup, de-duplicated and ordered
    * canonically. Empty when the measure is undeclared or every declaration on it
    * is in error.
    */
   grains: PreaggregateGrain[];
   /**
    * One entry per unusable `#@ preaggregate` line. A measure can have a good
    * grain and a bad one at once, so this is independent of `grains`.
    */
   errors: PreaggregateDeclarationError[];
   /** Tag parse diagnostics, surfaced by the caller the way build_plan does. */
   parseLog: LogMessage[];
}

/** The subset of a measure's `FieldDef` this reader needs. */
export interface AnnotatableMeasure {
   name: string;
   as?: string;
   // The IR's AnnotationsDef; kept loose so callers pass FieldDefs straight in.
   annotations?: unknown;
}

/**
 * Split a `grain=` value into canonical dimensions.
 *
 * Comma-separated, whitespace-insensitive, de-duplicated, sorted. A dotted
 * time truncation (`order_time.day`) is one dimension and is kept whole.
 */
export function parseGrainDimensions(grainText: string): string[] {
   const parts = grainText
      .split(",")
      .map((part) => part.trim())
      .filter((part) => part.length > 0);
   return [...new Set(parts)].sort();
}

/** A note that turns the annotation off. The one text pattern this module reads. */
const NEGATION = /#@\s*-\s*preaggregate\b/;

interface OrderedNote {
   text: string;
   /**
    * The note exactly as the IR gave it. Parsed as-is rather than rebuilt: a note
    * carries a source location that MOTLY dereferences while parsing (a
    * fabricated one throws), and passing the original through also means any
    * diagnostic points at the author's real line.
    */
   raw: unknown;
}

/**
 * A field's annotation notes in source order.
 *
 * Inherited notes first (the earlier layer), then the field's own block notes
 * and trailing notes. An extending source usually already carries the base's
 * notes in its own `blockNotes`, so `inherits` is belt-and-braces: a duplicate
 * grain de-duplicates, whereas a missed one would silently drop a rollup.
 */
function orderedNotes(annotations: unknown): OrderedNote[] {
   type Note = {
      text?: string;
      at?: { range?: { start?: { line?: number } } };
   };
   type Layer = { blockNotes?: Note[]; notes?: Note[]; inherits?: Layer };
   const layer = (annotations ?? {}) as Layer;
   const collect = (from: Layer | undefined): Note[] => [
      ...(from?.blockNotes ?? []),
      ...(from?.notes ?? []),
   ];
   return [...collect(layer.inherits), ...collect(layer)]
      .map((note) => ({ text: note.text ?? "", raw: note }))
      .filter((note) => note.text.length > 0);
}

/**
 * Read `#@ preaggregate` off one measure.
 *
 * Pure apart from the compiler's tag parse, and total: an unparseable annotation
 * degrades to undeclared with the diagnostics in `parseLog`, rather than
 * throwing into a package load.
 */
export function readPreaggregateAnnotation(
   measure: AnnotatableMeasure,
): PreaggregateDeclaration {
   const name = measure.as ?? measure.name;
   const parseLog: LogMessage[] = [];
   // Keyed on the canonical grain, so `grain="a, b"` and `grain="b, a"` on one
   // measure are one rollup rather than two identical tables.
   let grains = new Map<string, PreaggregateGrain>();
   let errors: PreaggregateDeclarationError[] = [];
   let declared = false;

   for (const note of orderedNotes(measure.annotations)) {
      if (NEGATION.test(note.text)) {
         // Everything declared above this line is off. Errors go too: an author
         // who negated a declaration should not be told to fix its grain.
         grains = new Map();
         errors = [];
         declared = false;
         continue;
      }

      let tag;
      try {
         const parsed = new Annotations(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            { notes: [note.raw] } as any,
         ).parseAsTag("@");
         tag = parsed.tag;
         parseLog.push(...parsed.log);
      } catch {
         // Mirrors deriveAnnotationFields: a malformed annotation must not take
         // the package down. An unreadable line cannot have declared anything.
         continue;
      }
      if (!tag.has("preaggregate")) continue;
      declared = true;

      // Nested form wins; the documented sibling-key form is the fallback. Both
      // parse per line, so this is the same precedence the merged tag applied.
      const grainText = tag.text("preaggregate", "grain") ?? tag.text("grain");
      // Same precedence as `grain`. Binds to THIS line's grain; a line carrying
      // only a namespace is a missing grain like any other.
      const namespaceText =
         tag.text("preaggregate", "namespace") ?? tag.text("namespace");
      let namespace: string | undefined;
      if (namespaceText !== undefined) {
         const candidate = namespaceText.trim();
         // Spliced bare into a generated `#@ persist name="…"`: a quote would end
         // the annotation's string, and a name needing quotes cannot be joined to
         // a generated table name (the CREATE and bind sides quote a mixed path
         // differently). Empty is refused, not ignored, per the strictness rule.
         if (!isSpliceableNamespace(candidate)) {
            errors.push({
               kind: "invalid_namespace",
               message: `Measure \`${name}\` declares \`#@ preaggregate namespace="${candidate}"\`, which is not a plain namespace. Use letters, digits, underscore, dollar or hyphen per part, dot-separated for a qualified one (\`analytics\`, \`my-project.analytics\`) — the rollup's own table name is generated and appended, so a namespace needing quotes cannot be joined to it.`,
            });
            continue;
         }
         namespace = candidate;
      }
      // Same precedence as `grain`, bound to THIS line's grain.
      const storageText =
         tag.text("preaggregate", "storage") ?? tag.text("storage");
      let storage: string | undefined;
      if (storageText !== undefined) {
         const candidate = storageText.trim();
         // Empty is refused, per the strictness rule. The NAME is not validated
         // here: which destinations exist is a deployment fact, so an unknown one
         // is refused downstream by the storage-destination resolver.
         if (candidate === "") {
            errors.push({
               kind: "empty_storage",
               message: `Measure \`${name}\` declares \`#@ preaggregate storage=""\` with no destination. Name the storage destination the rollup should be built into, or remove the key to build it alongside its base.`,
            });
            continue;
         }
         // A quote or backslash cannot survive re-emission into the synthesized
         // `#@ persist storage="…"` string literal; every other character can.
         if (/["\\]/.test(candidate)) {
            errors.push({
               kind: "invalid_storage",
               message: `Measure \`${name}\` declares \`#@ preaggregate storage=\` naming a destination that contains a quote or backslash. Publisher re-declares the destination on the rollup it synthesizes, and such a name cannot be written there. Rename the destination.`,
            });
            continue;
         }
         storage = candidate;
      }
      if (grainText === undefined) {
         errors.push({
            kind: "missing_grain",
            message: `Measure \`${name}\` is annotated \`#@ preaggregate\` without a grain. Add one, as \`#@ preaggregate grain="…"\`, naming the dimensions the rollup should be built at.`,
         });
         continue;
      }
      const dimensions = parseGrainDimensions(grainText);
      if (dimensions.length === 0) {
         errors.push({
            kind: "empty_grain",
            message: `Measure \`${name}\` declares \`#@ preaggregate\` with an empty grain (\`grain="${grainText}"\`). Name at least one dimension, or remove the annotation.`,
         });
         continue;
      }
      grains.set(dimensions.join("\u0000"), {
         dimensions,
         text: grainText,
         namespace,
         storage,
      });
   }

   return {
      declared,
      // Ordered by the canonical grain so a caller's output does not depend on
      // the order the annotations happened to be written in.
      grains: [...grains.values()].sort((a, b) =>
         a.dimensions.join("\u0000").localeCompare(b.dimensions.join("\u0000")),
      ),
      errors,
      parseLog,
   };
}
