// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(classification) "<class>"` on a field, `#(carries) ["<class>", …]` on a
 * source, and `#(control) ["<id>", …]` on either: what a field is, what an
 * entry point may expose, and which of the policy's controls a thing
 * implements, all in the words of the package's access policy. Values are
 * quoted and lists are bracketed and comma-separated, read by Malloy's tag
 * parser (`readTagList`). Publisher reads the shape and never
 * the vocabulary: `phi.direct_identifier`, `confidential`, `pci.pan`,
 * `soc2:CC6.1` and `nist:AC-3` are all just text to it, and which values a
 * package may use is the policy's to say and a review's to check. What
 * Publisher does with them:
 *

 * - **Sets.** A field carries a SET of classes, as a quoted list
 *   (`#(classification) ["phi.direct_identifier", "gdpr.personal"]`), because one column commonly sits under several regimes at
 *   once (an SSN is a HIPAA identifier, GDPR personal data and NIST PII).
 * - **Lineage.** A field with no class of its own takes the union of the
 *   classes of the fields its expression reads
 *   ({@link readSourceClassifications}): a `dimension: copied is ssn`, a
 *   `concat(first, spend)`, a `count(ssn)` all carry what they read, through
 *   a `rename:` and through a join. A declared set always wins, with the
 *   inputs still reported, so `birth_year is BIRTHDATE.year` can say it is a
 *   quasi-identifier derived from a direct one and a review can still see
 *   the derivation.
 * - **Discovery.** `get_context` reports the effective set on every
 *   dimension and measure card, with the inputs it came from when inferred
 *   (`package_entities.ts`).
 * - **Coverage.** The package load warns `classification-coverage` on every
 *   published source that still has an unclassified dimension or measure,
 *   once ANY field in the package carries a class
 *   ({@link classificationWarnings}). Self-declaring: a package that
 *   classifies nothing is not warned about. A malformed annotation warns
 *   `classification-malformed` naming the field and the fix.
 * - **Ceiling.** `#(carries)` on a source lists the classes a caller may
 *   reach from it. A field reachable from that source (own, inherited,
 *   inferred, or through a join) with any class off the list, or no class at
 *   all, is a leak, and a leak refuses the package load
 *   ({@link classificationLeaks}), the way a gate that does not parse does.
 * - **Controls.** `#(control)` tokens are collected per source and per field
 *   for the governance export (`governance.ts`), which is the control-to-
 *   implementation map a pipeline keeps and an auditor reads.
 *
 * Why the bracketed route and not a `# classification=` tag: it sits beside
 * `#(doc)` and the gates, the other annotations an agent reads for meaning
 * and policy, and Malloy claims no interpretation of it. The one rule a route
 * imposes is that the tokens follow the closing parenthesis, since Malloy
 * ends a route at the first whitespace: `#(classification phi)` warns
 * `malformed-route` at compile; `#(classification) "phi"` is the form.
 */

import type { ModelDef } from "@malloydata/malloy";
import { parseAnnotation } from "@malloydata/malloy-tag";
import type { components } from "../api";
import { annotationTexts, type AnnotationsDef } from "./annotations";
import type { Model } from "./model";

type ApiPackageWarning = NonNullable<
   components["schemas"]["Package"]["warnings"]
>[number];

/** `PackageWarning.code` of a published source with unclassified fields. */
export const CLASSIFICATION_COVERAGE = "classification-coverage";
/** `PackageWarning.code` of a `#(classification)` or `#(control)` annotation that names nothing usable. */
export const CLASSIFICATION_MALFORMED = "classification-malformed";

const CLASSIFICATION_LINE = /^#\(classification\)(.*)$/s;
const CARRIES_LINE = /^#\(carries\)(.*)$/s;
const CONTROL_LINE = /^#\(control\)(.*)$/s;

/** One annotation note, as `to_stable` (`{value}`) or a raw string carries it. */
type AnnotationText = string | { value: string };

function annotationText(a: AnnotationText): string {
   return typeof a === "string" ? a : a.value;
}

/** Whether an annotation line is a `#(classification)`, `#(carries)` or `#(control)` note. */
export function isClassificationLine(text: string): boolean {
   const t = text.trim();
   return (
      CLASSIFICATION_LINE.test(t) ||
      CARRIES_LINE.test(t) ||
      CONTROL_LINE.test(t)
   );
}

/** Codepoint order, so two servers on identical bytes agree. */
const byCodepoint = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/** A sorted set of tokens. */
function tokenSet(tokens: Iterable<string>): string[] {
   return Array.from(new Set(tokens)).sort(byCodepoint);
}

/**
 * Read an annotation's content as a tag value: a quoted string (`"x"`) or a
 * quoted, comma-separated list (`["x", "y"]`), the form every list-valued
 * annotation in Publisher uses (`tiles=[…]`, `# drill { to=[…] }`). Parsed by
 * Malloy's own tag parser, so a malformed list is a parse error with its
 * reason, never a silent split. `values` is a sorted set; `malformed` says
 * what is wrong in the words the load uses.
 */
export function readTagList(
   route: string,
   content: string,
   { list }: { list: "required" | "optional" },
): { values: string[]; malformed?: string } {
   const trimmed = content.trim();
   const example =
      route === "carries"
         ? '["phi.clinical", "none"]'
         : route === "control"
           ? '["AC-01", "soc2:CC6.1"]'
           : '"phi.direct_identifier" or ["phi.direct_identifier", "gdpr.personal"]';
   const fault = (why: string) => ({
      values: [] as string[],
      malformed: `has a #(${route}) annotation that ${why}; write ${list === "required" ? "a quoted, comma-separated list" : "a quoted string or a quoted, comma-separated list"}, such as #(${route}) ${example}`,
   });
   if (trimmed === "") return fault("names nothing");
   // An environment reference would be hydrated from the server's own
   // environment by the tag parser; never read one here.
   if (/@env\./.test(trimmed)) return fault("reads the environment");
   const isList = trimmed.startsWith("[");
   if (!isList && !trimmed.startsWith('"')) {
      return fault(`is not quoted ("${trimmed}")`);
   }
   if (!isList && list === "required") return fault("is not a list");
   let parsed: ReturnType<typeof parseAnnotation>;
   try {
      parsed = parseAnnotation(`# v=${trimmed}`);
   } catch (error) {
      return fault(
         `does not parse (${error instanceof Error ? error.message : String(error)})`,
      );
   }
   if (parsed.log.length > 0) {
      return fault(`does not parse (${parsed.log[0].message})`);
   }
   const items = isList
      ? parsed.tag.array("v")?.map((item) => item.text())
      : [parsed.tag.text("v")];
   if (!items || items.some((item) => typeof item !== "string")) {
      return fault("holds something other than quoted text");
   }
   const values = tokenSet((items as string[]).map((v) => v.trim()));
   if (values.length === 0) return fault("names nothing");
   if (values.some((v) => v === "")) return fault("names an empty value");
   return { values };
}

/** The content after a route, from every note on that route. */
function routeContents(
   texts: ReadonlyArray<string>,
   pattern: RegExp,
): string[] {
   return texts
      .map((text) => pattern.exec(text.trim()))
      .filter((m): m is RegExpExecArray => m !== null)
      .map((m) => m[1]);
}

/**
 * The classes a field's `#(classification)` declares: one quoted class, or a
 * quoted list when the field sits under several regimes at once. One note per
 * field; a second is `malformed`, since a list says it in one place.
 * An empty `values` with no `malformed` means the field declares nothing.
 */
export function readClassification(
   annotations: ReadonlyArray<AnnotationText> | undefined,
): { values: string[]; malformed?: string } {
   if (!annotations || annotations.length === 0) return { values: [] };
   const contents = routeContents(
      annotations.map(annotationText),
      CLASSIFICATION_LINE,
   );
   if (contents.length === 0) return { values: [] };
   if (contents.length > 1) {
      return {
         values: [],
         malformed:
            `has ${contents.length} #(classification) annotations; a field ` +
            `has one, listing every class it carries, such as ` +
            `#(classification) ["phi.direct_identifier", "gdpr.personal"]`,
      };
   }
   return readTagList("classification", contents[0], { list: "optional" });
}

/**
 * The control identifiers a `#(control)` lists, as a sorted set: any text,
 * in any framework's format (`"CC6.1"`, `"AC-3"`, `"164.312(a)(1)"`,
 * `"soc2:CC6.1"`). A quoted list; a single quoted identifier is accepted too.
 * Several notes on one level combine.
 */
export function readControls(annotations: ReadonlyArray<string> | undefined): {
   values: string[];
   malformed?: string;
} {
   if (!annotations || annotations.length === 0) return { values: [] };
   const values: string[] = [];
   const faults: string[] = [];
   for (const content of routeContents(annotations, CONTROL_LINE)) {
      const read = readTagList("control", content, { list: "optional" });
      values.push(...read.values);
      if (read.malformed) faults.push(read.malformed);
   }
   return {
      values: tokenSet(values),
      ...(faults.length > 0 ? { malformed: faults.join("; and ") } : {}),
   };
}

/**
 * The classes a source's `#(carries)` lists, read from the nearest
 * declaration: the source's own notes, else the base it extends, and so on,
 * the way a gate is inherited. `declared` is false when no level declares one.
 * `malformed` names a declaration that is not a non-empty quoted list, which
 * would make every field a leak and so is refused rather than enforced.
 */
export function readCarries(annotations: AnnotationsDef | undefined): {
   declared: boolean;
   classes: Set<string>;
   malformed?: string;
} {
   let node: AnnotationsDef | undefined = annotations;
   // Bounded: an `inherits` chain is finite, but a malformed IR must not hang.
   for (let hops = 0; node && hops < 64; hops++) {
      const contents = routeContents(ownTexts(node), CARRIES_LINE);
      if (contents.length > 0) {
         const classes = new Set<string>();
         for (const content of contents) {
            const read = readTagList("carries", content, { list: "required" });
            if (read.malformed) {
               return {
                  declared: true,
                  classes: new Set(),
                  malformed: read.malformed,
               };
            }
            for (const v of read.values) classes.add(v);
         }
         return { declared: true, classes };
      }
      node = node.inherits;
   }
   return { declared: false, classes: new Set() };
}

/** The note texts declared on ONE level of an annotation chain. */
function ownTexts(node: AnnotationsDef): string[] {
   return [...(node.blockNotes ?? []), ...(node.notes ?? [])].map(
      (note) => note.text,
   );
}

/**
 * The control identifiers a source declares on its OWN line, not those of a
 * base it extends: a control is a claim about what this source implements,
 * and an extension that replaces the base's gate does not implement the
 * base's control by inheritance.
 */
export function readSourceControls(annotations: AnnotationsDef | undefined): {
   values: string[];
   malformed?: string;
} {
   return readControls(annotations ? ownTexts(annotations) : undefined);
}

/** The slice of a compiled field the lineage reads. */
interface IrField {
   name?: string;
   as?: string;
   type?: string;
   join?: string;
   fields?: IrField[];
   e?: unknown;
   expressionType?: string;
   annotations?: AnnotationsDef;
   accessModifier?: string;
}

/** A classified field an expression reads, by its path from the source. */
export interface ClassifiedInput {
   field: string;
   classifications: string[];
}

/** What the lineage knows about one field of a source. */
export interface FieldClassification {
   /** The classes the field's own `#(classification)` lines declare. */
   declared: string[];
   /** The union of its inputs' classes, read when it declares none. */
   inferred: string[];
   /** The set in force: `declared` when non-empty, else `inferred`. */
   classifications: string[];
   /**
    * The classified fields the expression reads, nearest first: a direct
    * reference, or the inputs of an unclassified intermediate it reads.
    */
   inputs: ClassifiedInput[];
   /** The control identifiers the field's own `#(control)` lines name. */
   controls: string[];
   /** Why an annotation on the field is unusable, when one is. */
   malformed?: string;
   /** True for a field under `private:` / `internal:`, which callers never reach. */
   hidden: boolean;
   /** True for a dimension or measure; false for a view or a join. */
   isField: boolean;
   /** `measure` for an aggregate or analytic expression, else `dimension`. */
   kind: "dimension" | "measure";
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

function fieldKind(f: IrField): "dimension" | "measure" {
   const t = f.expressionType ?? "";
   return t.includes("aggregate") || t.includes("analytic")
      ? "measure"
      : "dimension";
}

/**
 * Every field reference in an IR tree, in walk order: an expression's
 * `{node: "field", path}` and a query's `{type: "fieldref", path}`. Positions
 * are skipped; nothing a reference hides in.
 */
export function fieldPathsIn(tree: unknown): string[][] {
   return fieldReferences(tree);
}

/** Every `{node: "field", path}` or `{type: "fieldref", path}` a tree reads. */
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
      if (
         (obj.node === "field" || obj.type === "fieldref") &&
         Array.isArray(obj.path)
      ) {
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
      const texts = annotationTexts(field.annotations);
      const own = readClassification(texts);
      const controls = readControls(texts);
      const inputs: ClassifiedInput[] = [];
      const seenInput = new Set<string>();
      for (const ref of fieldReferences(field.e)) {
         const refPath = owner ? `${owner}.${ref.join(".")}` : ref.join(".");
         if (refPath === path) continue;
         const input = resolve(refPath);
         if (!input) continue;
         // A classified input is an input; an unclassified one contributes
         // the inputs IT was read from, so lineage crosses an intermediate.
         const carried =
            input.classifications.length > 0
               ? [{ field: refPath, classifications: input.classifications }]
               : input.inputs;
         for (const c of carried) {
            if (seenInput.has(c.field)) continue;
            seenInput.add(c.field);
            inputs.push(c);
         }
      }
      visiting.delete(path);
      const inferred =
         own.values.length > 0
            ? []
            : tokenSet(inputs.flatMap((i) => i.classifications));
      const faults = [own.malformed, controls.malformed].filter(
         (f): f is string => f !== undefined,
      );
      const result: FieldClassification = {
         declared: own.values,
         inferred,
         classifications: own.values.length > 0 ? own.values : inferred,
         inputs,
         controls: controls.values,
         ...(faults.length > 0 ? { malformed: faults.join("; and ") } : {}),
         hidden: field.accessModifier !== undefined,
         isField: isAtomicField(field),
         kind: fieldKind(field),
      };
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
export function isDeniedToAll(apiSource: {
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
export interface PublishedSource {
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
export function publishedSources(
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
 * annotations are reported whether or not coverage is in force, since each
 * was written on purpose.
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
      const sourceControls = readSourceControls(s.ir.annotations);
      if (sourceControls.malformed) {
         warnings.push({
            model: s.modelPath,
            subject: s.source,
            code: CLASSIFICATION_MALFORMED,
            severity: "warn",
            message: `Source ${s.source} ${sourceControls.malformed}.`,
         });
      }
      for (const [path, c] of s.fields) {
         if (!c.malformed) continue;
         warnings.push({
            model: s.modelPath,
            subject: s.source,
            code: CLASSIFICATION_MALFORMED,
            severity: "warn",
            message: `Field ${path} of source ${s.source} ${c.malformed}.`,
         });
      }
   }
   const inForce = sources.some((s) =>
      s.fields.some(([, c]) => c.classifications.length > 0),
   );
   if (!inForce) return warnings;
   for (const s of sources) {
      const unclassified = s.fields
         .filter(([, c]) => c.classifications.length === 0)
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
            `nothing to say about it. Write #(classification) "<class>" above ` +
            `each; a field reached through a join, or derived from a ` +
            `classified field, is classified where that field is declared.`,
      });
   }
   return warnings;
}

/**
 * Every leak in the package: a field reachable from a source with a
 * `#(carries)` ceiling with a class not on the list, or with no class at
 * all, plus a `#(carries)` that names nothing. One message per finding, in
 * the words the author fixes it by. The load refuses the package when there
 * are any ({@link Package.formatClassificationLeaks}), so an entry point never
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
      const allowed = Array.from(carries.classes).sort(byCodepoint).join(", ");
      for (const [path, c] of s.fields) {
         const off = c.classifications.filter((k) => !carries.classes.has(k));
         if (c.classifications.length > 0 && off.length === 0) continue;
         if (c.classifications.length > 0) {
            const via =
               c.inferred.length > 0 && c.inputs.length > 0
                  ? `, inferred from ${c.inputs
                       .map(
                          (i) => `${i.field} (${i.classifications.join(" ")})`,
                       )
                       .join(", ")}`
                  : "";
            messages.push(
               `${where} carries [${allowed}] but exposes ${path}, classified ` +
                  `${c.classifications.join(" ")}${via}. Remove the field ` +
                  `from the source, derive one of a class the source carries ` +
                  `and declare it, or add the class to #(carries) if the ` +
                  `policy allows it.`,
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
