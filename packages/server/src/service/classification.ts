// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(classification) <class>` on a field and `#(carries) <class>…` on a
 * source: what a field is, and what a surface may expose, in the words of
 * the package's access policy. Publisher reads the shape and never the
 * vocabulary: `phi.direct_identifier`, `confidential`, `pci.pan` are all one
 * token to it, and which tokens a package may use is the policy's to say and
 * a review's to check. What Publisher does with them:
 *
 * - **Lineage.** A field with no class of its own takes the class of the
 *   fields its expression reads ({@link readSourceClassifications}): a
 *   `dimension: copied is ssn`, a `concat('***', ssn)`, a `count(ssn)` all
 *   carry `ssn`'s class, through a `rename:` and through a join. When the
 *   inputs disagree the field is reported as unclassified with the input
 *   classes named, because ranking classes is the policy's job, not the
 *   server's. A declared class always wins, with the inputs still reported,
 *   so `birth_year is BIRTHDATE.year` can say it is a quasi-identifier
 *   derived from a direct one and a review can still see the derivation.
 * - **Discovery.** `get_context` reports the effective class on every
 *   dimension and measure card, with the inputs it came from when it was
 *   inferred (`package_entities.ts`).
 * - **Coverage.** The package load warns `classification-coverage` on every
 *   published source that still has an unclassified dimension or measure,
 *   once ANY field in the package carries a class
 *   ({@link classificationWarnings}). Self-declaring: a package that
 *   classifies nothing is not warned about. A malformed annotation warns
 *   `classification-malformed` naming the field and the fix.
 * - **Ceiling.** `#(carries)` on a source lists the classes a caller may
 *   reach from it. A field reachable from that source (own, inherited,
 *   inferred, or through a join) whose class is not on the list, or that has
 *   no class at all, is a leak, and a leak refuses the package load
 *   ({@link classificationLeaks}), the way a gate that does not parse does.
 *
 * Why the bracketed route and not a `# classification=` tag: it sits beside
 * `#(doc)` and the gates, the other annotations an agent reads for meaning
 * and policy, and Malloy claims no interpretation of it. The one rule a route
 * imposes is that the class follows the closing parenthesis, since Malloy
 * ends a route at the first whitespace: `#(classification phi)` warns
 * `malformed-route` at compile; `#(classification) phi` is the form.
 */

import type { ModelDef } from "@malloydata/malloy";
import type { components } from "../api";
import { annotationTexts, type AnnotationsDef } from "./annotations";
import type { Model } from "./model";

type ApiPackageWarning = NonNullable<
   components["schemas"]["Package"]["warnings"]
>[number];

/** `PackageWarning.code` of a published source with unclassified fields. */
export const CLASSIFICATION_COVERAGE = "classification-coverage";
/** `PackageWarning.code` of a `#(classification)` annotation that names no single class. */
export const CLASSIFICATION_MALFORMED = "classification-malformed";

const CLASSIFICATION_LINE = /^#\(classification\)(.*)$/s;
const CARRIES_LINE = /^#\(carries\)(.*)$/s;

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

/**
 * The classes a source's `#(carries)` lists, read from the nearest
 * declaration: the source's own notes, else the base it extends, and so on,
 * the way a gate is inherited. `declared` is false when no level declares one.
 * `malformed` names a declaration with no class, which would make every field
 * a leak and so is refused rather than enforced.
 */
export function readCarries(annotations: AnnotationsDef | undefined): {
   declared: boolean;
   classes: Set<string>;
   malformed?: string;
} {
   let node: AnnotationsDef | undefined = annotations;
   // Bounded: an `inherits` chain is finite, but a malformed IR must not hang.
   for (let hops = 0; node && hops < 64; hops++) {
      const texts = [...(node.blockNotes ?? []), ...(node.notes ?? [])].map(
         (note) => note.text,
      );
      const lines = texts
         .map((text) => CARRIES_LINE.exec(text.trim()))
         .filter((m): m is RegExpExecArray => m !== null)
         .map((m) => m[1].trim());
      if (lines.length > 0) {
         const classes = new Set(
            lines.flatMap((line) => line.split(/\s+/).filter(Boolean)),
         );
         if (classes.size === 0) {
            return {
               declared: true,
               classes,
               malformed:
                  "has a #(carries) annotation that names no class; list the " +
                  "classes a caller may reach from this source, or delete it",
            };
         }
         return { declared: true, classes };
      }
      node = node.inherits;
   }
   return { declared: false, classes: new Set() };
}

/** The slice of a compiled field the lineage reads. */
interface IrField {
   name?: string;
   as?: string;
   type?: string;
   join?: string;
   fields?: IrField[];
   e?: unknown;
   annotations?: AnnotationsDef;
   accessModifier?: string;
}

/** A classified field an expression reads, by its path from the source. */
export interface ClassifiedInput {
   field: string;
   classification: string;
}

/** What the lineage knows about one field of a source. */
export interface FieldClassification {
   /** The class the field's own `#(classification)` declares. */
   declared?: string;
   /** The class its inputs agree on, when it declares none. */
   inferred?: string;
   /** `declared ?? inferred`: the class in force. */
   classification?: string;
   /**
    * The classified fields the expression reads, nearest first: a direct
    * reference, or the inputs of an unclassified intermediate it reads.
    */
   inputs: ClassifiedInput[];
   /** Why the field has no usable class although something was written or read. */
   malformed?: string;
   /** True for a field under `private:` / `internal:`, which callers never reach. */
   hidden: boolean;
   /** True for a dimension or measure; false for a view or a join. */
   isField: boolean;
}

/** How many joins deep a source's reachable fields are followed. */
export const MAX_JOIN_DEPTH = 3;

/** How many unclassified field paths one warning names before counting the rest. */
const MAX_NAMED_FIELDS = 12;

function activeName(f: IrField): string | undefined {
   return f.as ?? f.name;
}

function isJoinField(f: IrField): boolean {
   return typeof f.join === "string" && Array.isArray(f.fields);
}

function isAtomicField(f: IrField): boolean {
   return !isJoinField(f) && f.type !== "turtle" && !Array.isArray(f.fields);
}

/** Every `{node: "field", path}` an expression tree reads, in walk order. */
function fieldReferences(e: unknown): string[][] {
   const out: string[][] = [];
   const seen = new Set<object>();
   const visit = (node: unknown): void => {
      if (!node || typeof node !== "object" || seen.has(node)) return;
      seen.add(node);
      if (Array.isArray(node)) {
         for (const item of node) visit(item);
         return;
      }
      const obj = node as Record<string, unknown>;
      if (obj.node === "field" && Array.isArray(obj.path)) {
         out.push(obj.path.map(String));
      }
      for (const [key, value] of Object.entries(obj)) {
         // Positions are not expressions, and nothing a reference hides in.
         if (key === "at" || key === "location") continue;
         visit(value);
      }
   };
   visit(e);
   return out;
}

/**
 * The classification of every field reachable from a source, keyed by its
 * path from the source (`ssn`, `patient.ssn`), joins followed
 * {@link MAX_JOIN_DEPTH} deep. Hidden fields are in the map, because a
 * public field may be derived from one; `hidden` says so. A reference an
 * expression makes is resolved relative to the struct the field lives in, so
 * `patient.full_name is concat(first, last)` reads `patient.first`.
 */
export function readSourceClassifications(
   fields: IrField[] | undefined,
): Map<string, FieldClassification> {
   const byPath = new Map<string, { field: IrField; owner: string }>();
   const collect = (list: IrField[], owner: string, depth: number): void => {
      for (const field of list) {
         const name = activeName(field);
         if (!name) continue;
         const path = owner ? `${owner}.${name}` : name;
         byPath.set(path, { field, owner });
         if (isJoinField(field) && depth < MAX_JOIN_DEPTH) {
            collect(field.fields ?? [], path, depth + 1);
         }
      }
   };
   collect(fields ?? [], "", 0);

   const resolved = new Map<string, FieldClassification>();
   const visiting = new Set<string>();
   const resolve = (path: string): FieldClassification | undefined => {
      const done = resolved.get(path);
      if (done) return done;
      const entry = byPath.get(path);
      if (!entry) return undefined;
      // A cycle cannot occur in valid Malloy; guarded so a malformed model
      // means "no lineage", not a hang.
      if (visiting.has(path)) return undefined;
      visiting.add(path);
      const { field, owner } = entry;
      const own = readClassification(annotationTexts(field.annotations));
      const inputs: ClassifiedInput[] = [];
      const seenInput = new Set<string>();
      for (const ref of fieldReferences(field.e)) {
         const refPath = owner ? `${owner}.${ref.join(".")}` : ref.join(".");
         if (refPath === path) continue;
         const input = resolve(refPath);
         if (!input) continue;
         // A classified input is an input; an unclassified one contributes
         // the inputs IT was read from, so lineage crosses an intermediate.
         const carried = input.classification
            ? [{ field: refPath, classification: input.classification }]
            : input.inputs;
         for (const c of carried) {
            if (seenInput.has(c.field)) continue;
            seenInput.add(c.field);
            inputs.push(c);
         }
      }
      visiting.delete(path);
      const result: FieldClassification = {
         inputs,
         hidden: field.accessModifier !== undefined,
         isField: isAtomicField(field),
      };
      if (own.malformed) {
         result.malformed = own.malformed;
      } else if (own.value) {
         result.declared = own.value;
         result.classification = own.value;
      } else {
         const classes = Array.from(
            new Set(inputs.map((i) => i.classification)),
         );
         if (classes.length === 1) {
            result.inferred = classes[0];
            result.classification = classes[0];
         } else if (classes.length > 1) {
            result.malformed = `is derived from fields of differing classes (${inputs
               .map((i) => `${i.field}: ${i.classification}`)
               .join(", ")}); declare which class it carries`;
         }
      }
      resolved.set(path, result);
      return result;
   };
   for (const path of byPath.keys()) resolve(path);
   return resolved;
}

/** The compiled definition of a top-level source, by its active name. */
export function findIrSource(
   modelDef: ModelDef | undefined,
   sourceName: string,
): IrField | undefined {
   const contents = modelDef?.contents;
   if (!contents) return undefined;
   return Object.values(contents).find(
      (candidate) => activeName(candidate as IrField) === sourceName,
   ) as IrField | undefined;
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

/** One published source and what the lineage found on it. */
interface PublishedSource {
   modelPath: string;
   source: string;
   ir: IrField;
   deniedToAll: boolean;
   /** Reachable dimensions and measures a caller can read, by path. */
   fields: Array<[string, FieldClassification]>;
}

/**
 * Every source the package publishes, with its reachable fields classified.
 * `isListed` says which models are on the surface, the same answer
 * `listModels()` gives; `getSourceInfos()` says which sources a listed model
 * exports. A model with no compiled definition contributes nothing.
 */
function publishedSources(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
): PublishedSource[] {
   const out: PublishedSource[] = [];
   for (const [modelPath, model] of models) {
      if (!isListed(modelPath)) continue;
      let sourceInfos: ReturnType<Model["getSourceInfos"]>;
      let apiSources: ReturnType<Model["getSources"]>;
      let modelDef: ModelDef | undefined;
      try {
         sourceInfos = model.getSourceInfos();
         apiSources = model.getSources();
         modelDef = model.getModelDef();
      } catch {
         continue;
      }
      for (const sourceInfo of sourceInfos ?? []) {
         const ir = findIrSource(modelDef, sourceInfo.name);
         if (!ir) continue;
         const apiSource = apiSources?.find((s) => s.name === sourceInfo.name);
         const lineage = readSourceClassifications(ir.fields);
         out.push({
            modelPath,
            source: sourceInfo.name,
            ir,
            deniedToAll: apiSource ? isDeniedToAll(apiSource) : false,
            fields: Array.from(lineage).filter(
               ([, c]) => c.isField && !c.hidden,
            ),
         });
      }
   }
   return out;
}

/**
 * The classification warnings over a package's published models. Fields of
 * hidden models are only reported where a listed source reaches them through
 * a join, under that dotted path. A source locked with an unconditional deny
 * publishes nothing a caller can read, so its own fields are not demanded a
 * class (its fields reached through a join from an open source are).
 *
 * Coverage is self-declaring: `classification-coverage` fires only once some
 * field of some listed model carries a class, declared or inferred. Malformed
 * annotations, and fields whose inputs disagree, are reported whether or not
 * coverage is in force, since each was written on purpose.
 */
export function classificationWarnings(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
): ApiPackageWarning[] {
   const sources = publishedSources(models, isListed).filter(
      (s) => !s.deniedToAll,
   );
   const warnings: ApiPackageWarning[] = [];
   for (const s of sources) {
      for (const [path, c] of s.fields) {
         if (!c.malformed) continue;
         warnings.push({
            model: s.modelPath,
            subject: s.source,
            code: CLASSIFICATION_MALFORMED,
            severity: "warn",
            message:
               `Field ${path} of source ${s.source} ${c.malformed}. Write ` +
               `"#(classification) <class>" on its own line above the field, ` +
               `with the class your policy names.`,
         });
      }
   }
   const inForce = sources.some((s) =>
      s.fields.some(([, c]) => c.classification !== undefined),
   );
   if (!inForce) return warnings;
   for (const s of sources) {
      const unclassified = s.fields
         .filter(([, c]) => !c.classification && !c.malformed)
         .map(([path]) => path);
      if (unclassified.length === 0) continue;
      const named = unclassified.slice(0, MAX_NAMED_FIELDS).join(", ");
      const more = unclassified.length - MAX_NAMED_FIELDS;
      warnings.push({
         model: s.modelPath,
         subject: s.source,
         code: CLASSIFICATION_COVERAGE,
         severity: "warn",
         message:
            `Source ${s.source} publishes ${unclassified.length} ` +
            `${unclassified.length === 1 ? "field" : "fields"} with no ` +
            `#(classification): ${named}${more > 0 ? ` (+${more} more)` : ""}. ` +
            `This package classifies its fields, so every dimension and ` +
            `measure a caller can read needs a class, or the policy has ` +
            `nothing to say about it. Write "#(classification) <class>" above ` +
            `each; a field reached through a join, or derived from a ` +
            `classified field, is classified where that field is declared.`,
      });
   }
   return warnings;
}

/**
 * Every leak in the package: a field reachable from a source with a
 * `#(carries)` ceiling whose class is not on the list, or that has no class at
 * all, plus a `#(carries)` that names nothing. One message per finding, in
 * the words the author fixes it by. The load refuses the package when there
 * are any ({@link Package.formatClassificationLeaks}), so a surface never
 * serves what it says it does not carry.
 */
export function classificationLeaks(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
): string[] {
   const messages: string[] = [];
   for (const s of publishedSources(models, isListed)) {
      const carries = readCarries(s.ir.annotations);
      if (!carries.declared) continue;
      const where = `source ${s.source} (${s.modelPath})`;
      if (carries.malformed) {
         messages.push(`${where} ${carries.malformed}.`);
         continue;
      }
      const allowed = Array.from(carries.classes).join(", ");
      for (const [path, c] of s.fields) {
         if (c.classification && carries.classes.has(c.classification)) {
            continue;
         }
         if (c.classification) {
            const via =
               c.inferred && c.inputs.length > 0
                  ? `, inferred from ${c.inputs
                       .map((i) => `${i.field} (${i.classification})`)
                       .join(", ")}`
                  : "";
            messages.push(
               `${where} carries [${allowed}] but exposes ${path}, classified ` +
                  `${c.classification}${via}. Remove the field from the ` +
                  `surface, derive one of a class the surface carries and ` +
                  `declare it, or add the class to #(carries) if the policy ` +
                  `allows it.`,
            );
         } else {
            const why = c.malformed
               ? ` (it ${c.malformed})`
               : " (it declares none and reads no classified field)";
            messages.push(
               `${where} carries [${allowed}] but exposes ${path}, which has ` +
                  `no class${why}. Every field reachable from a source with ` +
                  `#(carries) needs a #(classification) on the list.`,
            );
         }
      }
   }
   return messages;
}
