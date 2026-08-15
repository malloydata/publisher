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
 * That is two rollups, not one, and the difference is cost rather than coverage.
 * A rollup does serve queries grouped by any SUBSET of its grain, so a single
 * rollup at `category, order_day` would answer both queries above correctly. But
 * a combined grain has roughly the product of its dimensions' cardinalities, so
 * it can approach the base table's row count and save nothing, while either grain
 * alone is small. Picking the smallest rollup that covers a query is the whole
 * mechanism; that only works if an author can declare more than one.
 *
 * ## Why this reads the annotation NOTES and not just the merged tag
 *
 * `#@ persist` is read through MOTLY's merged tag (`deriveAnnotationFields` in
 * build_plan.ts), which is the right default: merge order, precedence across an
 * extend chain and negation all come from the compiler rather than from anything
 * written here. But the merged tag holds one value per key, so two `grain=` lines
 * collapse to the LAST one and the rest vanish with no diagnostic — verified
 * against the pinned compiler. Several grains simply cannot be expressed through
 * it, so grains are collected from the individual notes instead.
 *
 * That means owning the ordering rules, which is a real cost, so it is kept to
 * the smallest possible set: **notes are processed in source order; a
 * `#@ preaggregate` line with a grain adds one; a `#@ -preaggregate` line clears
 * every grain accumulated so far.** Nothing else is interpreted.
 *
 * Two measured facts make that safe rather than a re-implementation:
 *
 *  - Annotations arrive in `blockNotes` in source order, each with its line, so
 *    "in order" is read from the IR and not reconstructed.
 *  - An extending source's fields carry the base's annotations in their OWN
 *    `blockNotes`, so inherited declarations are seen without walking a chain,
 *    and an extend that negates one still reads as `[declare, negate]` and
 *    therefore clears.
 *
 * One deliberate divergence from the merged tag, in the safe direction. Because
 * `#@ preaggregate grain="…"` parses as two sibling keys rather than a nested
 * property, the merged tag reports this as UNDECLARED while still holding
 * `grain="b"`:
 *
 *   #@ preaggregate grain="a"
 *   #@ -preaggregate
 *   #@ preaggregate grain="b"
 *
 * Read in order, the author declared `b` last, and that is what this returns. The
 * divergence can only ever turn something ON that the merged tag lost; a trailing
 * `#@ -preaggregate` clears everything, so negation is never overridden.
 *
 * ## Strictness
 *
 * A declaration that cannot be used is an error, not a silent skip: pre-
 * aggregation refuses at publish rather than quietly omitting, so an annotation
 * that does nothing must never be possible. Callers turn errors into a 400.
 */

import { Annotations } from "@malloydata/malloy";
import type { LogMessage } from "@malloydata/malloy";

/** One grain a measure is declared at — one rollup. */
export interface PreaggregateGrain {
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
}

/** A `#@ preaggregate` line that is present but unusable. */
export interface PreaggregateDeclarationError {
   kind: "missing_grain" | "empty_grain";
   /** Names the measure and the fix; becomes the body of a publish-time 400. */
   message: string;
}

/** What `#@ preaggregate` says about one measure. */
export interface PreaggregateDeclaration {
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
 * Inherited notes first (they are the earlier layer), then the field's own block
 * notes and any trailing notes. Measured: an extending source usually carries the
 * base's notes in its own `blockNotes` already, so `inherits` is belt-and-braces —
 * a duplicate grain de-duplicates and costs nothing, whereas a missed one would
 * silently drop a rollup.
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
      grains.set(dimensions.join("\u0000"), { dimensions, text: grainText });
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
