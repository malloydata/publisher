// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(classification) <class>`: the data class a field carries, as the
 * package's access policy names it. Publisher reads the shape and never the
 * vocabulary: `phi.direct_identifier`, `confidential`, `pci.pan` are all one
 * token to it, and which tokens a package may use is the policy's to say and
 * a review's to check. What Publisher does with it:
 *
 * - `get_context` reports it on every dimension and measure card as
 *   `classification`, so an agent sees the class of a field before reading it
 *   (`package_entities.ts`).
 * - The package load warns `classification-coverage` on every published
 *   source that still has an unclassified dimension or measure, once ANY
 *   field in the package carries a class ({@link classificationWarnings}).
 *   Self-declaring: a package that classifies nothing is not warned about,
 *   and a package that classifies one field has said it classifies.
 * - A malformed annotation (no class, or more than one token) warns
 *   `classification-malformed` naming the field and the fix.
 *
 * Why the bracketed route and not a `# classification=` tag: it sits beside
 * `#(doc)` and the gates, the other annotations an agent reads for meaning
 * and policy, and Malloy claims no interpretation of it. The one rule a route
 * imposes is that the class follows the closing parenthesis, since Malloy
 * ends a route at the first whitespace: `#(classification phi)` warns
 * `malformed-route` at compile; `#(classification) phi` is the form.
 */

import type { components } from "../api";
import type { Model } from "./model";

type ApiPackageWarning = NonNullable<
   components["schemas"]["Package"]["warnings"]
>[number];

/** `PackageWarning.code` of a published source with unclassified fields. */
export const CLASSIFICATION_COVERAGE = "classification-coverage";
/** `PackageWarning.code` of a `#(classification)` annotation that names no single class. */
export const CLASSIFICATION_MALFORMED = "classification-malformed";

const CLASSIFICATION_LINE = /^#\(classification\)(.*)$/s;

/** One annotation note, as `to_stable` (`{value}`) or a raw string carries it. */
type AnnotationText = string | { value: string };

function annotationText(a: AnnotationText): string {
   return typeof a === "string" ? a : a.value;
}

/** Whether an annotation line is a `#(classification)` note. */
export function isClassificationLine(text: string): boolean {
   return CLASSIFICATION_LINE.test(text.trim());
}

/**
 * The class a field's annotations declare. `value` is the one token after the
 * route; `malformed` says why there is none when a `#(classification)` note is
 * present but unusable, in the words the load warning uses. Both absent means
 * the field declares no classification.
 */
export function readClassification(
   annotations: ReadonlyArray<AnnotationText> | undefined,
): { value?: string; malformed?: string } {
   if (!annotations || annotations.length === 0) return {};
   const notes = annotations
      .map(annotationText)
      .map((text) => CLASSIFICATION_LINE.exec(text.trim()))
      .filter((m): m is RegExpExecArray => m !== null)
      .map((m) => m[1].trim());
   if (notes.length === 0) return {};
   if (notes.length > 1) {
      return {
         malformed: `carries ${notes.length} #(classification) annotations; a field has one class`,
      };
   }
   const tokens = notes[0].split(/\s+/).filter(Boolean);
   if (tokens.length === 0) {
      return {
         malformed: "has a #(classification) annotation that names no class",
      };
   }
   if (tokens.length > 1) {
      return {
         malformed:
            `has a #(classification) annotation with ${tokens.length} words ` +
            `("${notes[0]}"); a class is one token, such as phi.direct_identifier`,
      };
   }
   return { value: tokens[0] };
}

/** The slice of a SourceInfo schema field the lint reads. */
interface SchemaField {
   kind?: string;
   name: string;
   annotations?: AnnotationText[];
   schema?: { fields?: SchemaField[] };
}

/** How many joins deep the lint follows a published source's fields. */
const MAX_JOIN_DEPTH = 3;

/** How many unclassified field paths one warning names before counting the rest. */
const MAX_NAMED_FIELDS = 12;

interface SourceCoverage {
   modelPath: string;
   source: string;
   unclassified: string[];
   malformed: Array<{ path: string; why: string }>;
   classified: number;
}

function walkFields(
   fields: SchemaField[],
   prefix: string,
   depth: number,
   into: SourceCoverage,
): void {
   for (const field of fields) {
      const path = prefix ? `${prefix}.${field.name}` : field.name;
      if (field.kind === "join") {
         if (depth >= MAX_JOIN_DEPTH) continue;
         walkFields(field.schema?.fields ?? [], path, depth + 1, into);
         continue;
      }
      if (field.kind !== "dimension" && field.kind !== "measure") continue;
      const { value, malformed } = readClassification(field.annotations);
      if (malformed) into.malformed.push({ path, why: malformed });
      else if (value) into.classified += 1;
      else into.unclassified.push(path);
   }
}

/** Whether a compiled source is denied to every caller, so nothing it publishes is reachable. */
function isDeniedToAll(apiSource: {
   accessFilter?: string[];
   authorize?: string[];
}): boolean {
   const isDeny = (expr: string) => expr.trim().toLowerCase() === "false";
   return (
      (apiSource.accessFilter ?? []).some(isDeny) ||
      (apiSource.authorize ?? []).some(isDeny)
   );
}

/**
 * The classification warnings over a package's published models. `isListed`
 * says which models are on the package's surface, the same answer
 * `listModels()` gives; fields of hidden models are only reported where a
 * listed source reaches them through a join, under that dotted path. A source
 * locked with an unconditional deny publishes nothing a caller can read, so
 * its own fields are not demanded a class (its fields reached through a join
 * from an open source are).
 *
 * Coverage is self-declaring: `classification-coverage` fires only once some
 * field of some listed model carries a well-formed class. Malformed
 * annotations are reported whether or not coverage is in force, since each
 * was written on purpose.
 */
export function classificationWarnings(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
): ApiPackageWarning[] {
   const coverages: SourceCoverage[] = [];
   for (const [modelPath, model] of models) {
      if (!isListed(modelPath)) continue;
      let sourceInfos: ReturnType<Model["getSourceInfos"]>;
      let apiSources: ReturnType<Model["getSources"]>;
      try {
         sourceInfos = model.getSourceInfos();
         apiSources = model.getSources();
      } catch {
         continue;
      }
      for (const sourceInfo of sourceInfos ?? []) {
         const apiSource = apiSources?.find((s) => s.name === sourceInfo.name);
         const coverage: SourceCoverage = {
            modelPath,
            source: sourceInfo.name,
            unclassified: [],
            malformed: [],
            classified: 0,
         };
         const fields = (sourceInfo.schema?.fields ?? []) as SchemaField[];
         if (apiSource && isDeniedToAll(apiSource)) {
            // Only what a join from elsewhere could reach; that is reported
            // under the reaching source, so this one contributes nothing.
            continue;
         }
         walkFields(fields, "", 0, coverage);
         coverages.push(coverage);
      }
   }

   const warnings: ApiPackageWarning[] = [];
   for (const c of coverages) {
      for (const { path, why } of c.malformed) {
         warnings.push({
            model: c.modelPath,
            subject: c.source,
            code: CLASSIFICATION_MALFORMED,
            severity: "warn",
            message:
               `Field ${path} of source ${c.source} ${why}. Write ` +
               `"#(classification) <class>" on its own line above the field, ` +
               `with the class your policy names.`,
         });
      }
   }
   const inForce = coverages.some((c) => c.classified > 0);
   if (!inForce) return warnings;
   for (const c of coverages) {
      if (c.unclassified.length === 0) continue;
      const named = c.unclassified.slice(0, MAX_NAMED_FIELDS).join(", ");
      const more = c.unclassified.length - MAX_NAMED_FIELDS;
      warnings.push({
         model: c.modelPath,
         subject: c.source,
         code: CLASSIFICATION_COVERAGE,
         severity: "warn",
         message:
            `Source ${c.source} publishes ${c.unclassified.length} ` +
            `${c.unclassified.length === 1 ? "field" : "fields"} with no ` +
            `#(classification): ${named}${more > 0 ? ` (+${more} more)` : ""}. ` +
            `This package classifies its fields, so every dimension and ` +
            `measure a caller can read needs a class, or the policy has ` +
            `nothing to say about it. Write "#(classification) <class>" above ` +
            `each; a field reached through a join is classified where it is ` +
            `declared.`,
      });
   }
   return warnings;
}
