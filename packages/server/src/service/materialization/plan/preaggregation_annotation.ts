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
 * `name=` (or its deprecated spelling, `namespace=`) and `storage=` bind to the
 * grain on their own line and are cleared with it (a grain IS a table, so each
 * can be placed differently).
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

import { hasEnvReference } from "../../motly";
import { nearestKey } from "../../annotation_reader";
import { placementOf, readNotes, type RouteTag } from "./declaration";
import { readRollupContainer } from "./preaggregation_namespace";
import type { LogMessage } from "@malloydata/malloy";
import type { DeclaredFreshness } from "../domain/freshness";
import {
   FRESHNESS_GROUP_KEYS,
   readFreshnessGroup,
} from "./freshness_declaration";

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
    * same line: the container of its `name=` (`#@ preaggregate
    * grain="category" name="analytics.orders_rollup"`), or its deprecated
    * `namespace=`. The table name itself is always derived. Undefined when
    * unspecified, in which case the base's `name=` supplies it.
    */
   namespace?: string;
   /** {@link namespace} was written as the deprecated `namespace=`. */
   deprecatedNamespace?: true;
   /** Its `name=`'s leaf: the readable prefix of its table's name. */
   prefix?: string;
   /**
    * The storage destination this rollup's table is built into and served from,
    * when the author named one (`#@ preaggregate grain="category"
    * storage=lake`). Undefined means database: built into the base's own
    * warehouse.
    *
    * Unlike `namespace`, NOTHING supplies it when unspecified: a destination is
    * never inherited from the base's `#@ persist storage=` (see
    * `basePersistNamespace` for why).
    */
   storage?: string;
   /**
    * This grain's own `freshness` group, when its line writes one
    * (`#@ preaggregate grain="category" freshness.window="1h"`), read as
    * `#(preaggregate)` reads it (freshness_declaration.ts).
    */
   freshness?: DeclaredFreshness;
}

/** A `#@ preaggregate` line that is present but unusable. */
interface PreaggregateDeclarationError {
   kind:
      | "missing_grain"
      | "empty_grain"
      | "invalid_namespace"
      | "empty_storage"
      | "invalid_storage"
      | "unknown_key"
      | "unparseable"
      | "env_reference"
      | "invalid_freshness";
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

/** A note that turns the annotation off. */
const NEGATION = /#@\s*-\s*preaggregate\b/;

/**
 * A note that declares `#@ preaggregate`, told from its text when the line
 * cannot be read (it does not parse, or it reads the environment and the
 * reader drops it).
 */
const DECLARATION = /^\s*#@\s*preaggregate\b/;

/** The keys a `#@ preaggregate` line takes, beside the directive. A closed set. */
const LINE_KEYS: ReadonlySet<string> = new Set([
   "preaggregate",
   "grain",
   "name",
   "namespace",
   "storage",
   "freshness",
]);

/** The keys `#@ preaggregate { … }` takes inside the directive. */
const DIRECTIVE_KEYS: ReadonlySet<string> = new Set([
   "grain",
   "name",
   "namespace",
   "storage",
   "freshness",
]);

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
 * A `#@ preaggregate` line's `freshness` group, read as `#(preaggregate)`
 * reads it (freshness_declaration.ts); the nested form wins, as for grain=.
 */
function readLineFreshness(
   tag: RouteTag,
   name: string,
): { freshness?: DeclaredFreshness } | { error: PreaggregateDeclarationError } {
   const fresh = readFreshnessGroup(
      tag,
      tag.has("preaggregate", "freshness")
         ? ["preaggregate", "freshness"]
         : ["freshness"],
   );
   if ("unknownKeys" in fresh) {
      return {
         error: {
            kind: "unknown_key",
            message:
               `Measure \`${name}\` declares \`#@ preaggregate\` with ` +
               fresh.unknownKeys
                  .map((key) => {
                     const near = nearestKey(key, FRESHNESS_GROUP_KEYS);
                     return `\`freshness.${key}=\`${near ? ` (did you mean \`freshness.${near}=\`?)` : ""}`;
                  })
                  .join(", ") +
               `, which is not a key it reads. Use freshness.window= or freshness.fallback=.`,
         },
      };
   }
   if ("invalid" in fresh) {
      return {
         error: {
            kind: "invalid_freshness",
            message: `Measure \`${name}\` declares \`#@ preaggregate\` that ${fresh.invalid}.`,
         },
      };
   }
   return fresh;
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

      const declaring = DECLARATION.test(note.text);
      // The reader drops a line that reads the environment whole, so the
      // declaration would vanish: an invalid line, reported as one.
      if (declaring && hasEnvReference(note.text)) {
         declared = true;
         errors.push({
            kind: "env_reference",
            message: `Measure \`${name}\` declares \`#@ preaggregate\` on a line that reads the server's environment (\`@env.\`), which a materialization annotation may not: Publisher never reads it. Write the value into the model.`,
         });
         continue;
      }
      let tag;
      let parseErrors: string[];
      try {
         const parsed = readNotes([note.raw], "@");
         tag = parsed.tag;
         parseLog.push(...parsed.log);
         parseErrors = parsed.log
            .filter((entry) => entry.severity === "error")
            .map((entry) => entry.message);
      } catch (error) {
         // Never thrown into a package load: reported as a finding instead.
         tag = undefined;
         parseErrors = [error instanceof Error ? error.message : String(error)];
      }
      if (!tag || parseErrors.length > 0) {
         // A line that does not parse declares nothing it can be read for; on
         // a `#@ preaggregate` line that is an invalid declaration.
         if (declaring) {
            declared = true;
            errors.push({
               kind: "unparseable",
               message: `Measure \`${name}\` declares \`#@ preaggregate\` on a line that does not parse (${parseErrors.join("; ")}). Fix the line's syntax.`,
            });
         }
         continue;
      }
      if (!tag.has("preaggregate")) continue;
      declared = true;

      // The key set is closed: a typo (`stroage="lake"`) would otherwise
      // build the rollup somewhere its author did not say.
      const unknown = [
         ...[...tag.entries()]
            .map(([key]) => key)
            .filter((key) => !LINE_KEYS.has(key))
            .map((key) => ({ key, near: nearestKey(key, LINE_KEYS) })),
         ...[...(tag.tag("preaggregate")?.entries() ?? [])]
            .map(([key]) => key)
            .filter((key) => !DIRECTIVE_KEYS.has(key))
            .map((key) => ({ key, near: nearestKey(key, DIRECTIVE_KEYS) })),
      ];
      if (unknown.length > 0) {
         errors.push({
            kind: "unknown_key",
            message:
               `Measure \`${name}\` declares \`#@ preaggregate\` with ` +
               unknown
                  .map(
                     ({ key, near }) =>
                        `\`${key}=\`${near ? ` (did you mean \`${near}=\`?)` : ""}`,
                  )
                  .join(", ") +
               `, which is not a key it reads. Use grain=, name=, storage= or freshness.window= / freshness.fallback=.`,
         });
         continue;
      }

      const fresh = readLineFreshness(tag, name);
      if ("error" in fresh) {
         errors.push(fresh.error);
         continue;
      }

      // Nested form wins; the documented sibling-key form is the fallback. Both
      // parse per line, so this is the same precedence the merged tag applied.
      const grainText = tag.text("preaggregate", "grain") ?? tag.text("grain");
      // Same precedence as `grain`. Binds to THIS line's grain; a line carrying
      // only a placement is a missing grain like any other. Spliced bare into
      // a generated `#@ persist name="…"`: a quote would end the annotation's
      // string, and a name needing quotes cannot be joined to a generated
      // table name, so such a container is refused, not ignored.
      const placedKey = (key: string) =>
         tag.text("preaggregate", key) ?? tag.text(key);
      const read = readRollupContainer({
         name: placedKey("name"),
         namespace: placedKey("namespace"),
      });
      if ("invalid" in read) {
         errors.push({
            kind: "invalid_namespace",
            message: `Measure \`${name}\` declares \`#@ preaggregate\` that ${read.invalid}. Use letters, digits, underscore, dollar or hyphen per part, dot-separated for a qualified one (\`analytics\`, \`my-project.analytics\`): the rollup's own table name is generated and appended, so a container needing quotes cannot be joined to it.`,
         });
         continue;
      }
      const namespace = read.container;
      // The placement rule (declaration.ts), bound to THIS line's grain. The
      // NAME is not validated here: which destinations exist is a deployment
      // fact, so an unknown one is refused downstream by the
      // storage-destination resolver.
      const placed = placementOf(tag, "preaggregate");
      let storage: string | undefined;
      const placedIn =
         "refused" in placed
            ? ""
            : placed.placement.tier === "storage"
              ? placed.placement.destination
              : undefined;
      if (placedIn !== undefined) {
         const candidate = placedIn;
         // Names nothing: an invalid declaration, which fails the load.
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
         ...(read.deprecatedNamespace ? { deprecatedNamespace: true } : {}),
         ...(read.prefix !== undefined ? { prefix: read.prefix } : {}),
         storage,
         ...(fresh.freshness ? { freshness: fresh.freshness } : {}),
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
