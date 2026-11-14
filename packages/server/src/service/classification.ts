// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(classification) "<class>"` on a field and `#(carries) ["<class>", …]` on a
 * source: what a field is and what an entry point may expose, both in the
 * words of the package's access policy. Values are
 * quoted and lists are bracketed and comma-separated, read by Malloy's tag
 * parser (`readRouteList`, in annotation_reader.ts). Publisher reads the shape and never
 * the vocabulary: `phi.direct_identifier`, `confidential` and `pci.pan` are
 * all just text to it, and which values a
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
 *   once ANY field in the package carries a class (`classificationWarnings`
 *   in ./classification_report.ts). Self-declaring: a package that
 *   classifies nothing is not warned about. A malformed annotation warns
 *   `classification-malformed` naming the field and the fix.
 * - **Ceiling.** `#(carries)` on a source lists the classes a caller may
 *   reach from it. A field reachable from that source (own, inherited,
 *   inferred, or through a join) with any class off the list, or no class at
 *   all, is a leak, and a leak refuses the package load (`classificationLeaks`
 *   in ./classification_report.ts), the way a gate that does not parse does.
 *   A view counts too, for every class it groups by, aggregates or filters on,
 *   hidden fields included.
 * - **Downgrades.** A field that declares a lower class than the fields it
 *   reads warns `classification-downgrade`; under `#(carries)` a plain copy
 *   (a reference, optionally cast) that does so is refused, since a copy
 *   cannot de-identify anything.
 *
 * Why the bracketed route and not a `# classification=` tag: it sits beside
 * `#(doc)` and the gates, the other annotations an agent reads for meaning
 * and policy, and Malloy claims no interpretation of it. The one rule a route
 * imposes is that the tokens follow the closing parenthesis, since Malloy
 * ends a route at the first whitespace: `#(classification phi)` warns
 * `malformed-route` at compile; `#(classification) "phi"` is the form.
 */

import type { ModelDef, SourceDef } from "@malloydata/malloy";
import {
   byCodepoint,
   isOnRoute,
   payloadsOnRoute,
   readRouteList,
   tokenSet,
} from "./annotation_reader";
import type { components } from "../api";
import { annotationTexts, type AnnotationsDef } from "./annotations";
import {
   MAX_JOIN_PATHS,
   MAX_JOIN_RECURSION_DEPTH,
   type RefSummaryLike,
} from "./filter_binding_guard";
import {
   resolveCompositeResolvedBase,
   resolveQuerySourceBase,
} from "./governance/ir/source_graph";
import { forEachFieldPath } from "./governance/ir/array_query";

export type ApiPackageWarning = NonNullable<
   components["schemas"]["Package"]["warnings"]
>[number];

/** `PackageWarning.code` of a published source with unclassified fields. */
export const CLASSIFICATION_COVERAGE = "classification-coverage";
/** `PackageWarning.code` of a `#(classification)` or `#(carries)` annotation that names nothing usable. */
export const CLASSIFICATION_MALFORMED = "classification-malformed";
/** `PackageWarning.code` of a computed field that declares a lower class than the fields it reads. */
export const CLASSIFICATION_DOWNGRADE = "classification-downgrade";

/** One annotation note, as `to_stable` (`{value}`) or a raw string carries it. */
type AnnotationText = string | { value: string };

function annotationText(a: AnnotationText): string {
   return typeof a === "string" ? a : a.value;
}

/** Whether an annotation line is a `#(classification)` or `#(carries)` note. */
export function isClassificationLine(text: string): boolean {
   return isOnRoute(text, "classification") || isOnRoute(text, "carries");
}

/** Codepoint order, so two servers on identical bytes agree (see annotation_reader.ts). */
export { byCodepoint };

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
   const contents = payloadsOnRoute(
      annotations.map(annotationText),
      "classification",
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
   return readRouteList("classification", contents[0], { list: "optional" });
}

const MAX_CARRIES_HOPS = 64;

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
   for (let hops = 0; node; hops++) {
      if (hops >= MAX_CARRIES_HOPS) {
         return {
            declared: true,
            classes: new Set(),
            malformed: `has an annotation chain deeper than ${MAX_CARRIES_HOPS} levels, past which its #(carries) cannot be read`,
         };
      }
      const contents = payloadsOnRoute(ownTexts(node), "carries");
      if (contents.length > 0) {
         const classes = new Set<string>();
         for (const content of contents) {
            const read = readRouteList("carries", content, {
               list: "required",
            });
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

/** The slice of a compiled field the lineage reads. */
export interface IrField {
   name?: string;
   as?: string;
   type?: string;
   join?: string;
   fields?: IrField[];
   e?: unknown;
   expressionType?: string;
   annotations?: AnnotationsDef;
   accessModifier?: string;
   /** On a `->` query source: the query its fields are the output of. */
   query?: IrQuery;
   refSummary?: RefSummaryLike;
   pipeline?: IrStage[];
}

/** The slice of one field of a pipeline stage the lineage reads. */
interface IrQueryField {
   type?: string;
   name?: string;
   as?: string;
   path?: string[];
   e?: unknown;
   refSummary?: RefSummaryLike;
   annotations?: AnnotationsDef;
   /** On a `nest:`: the stages of the view it nests. */
   pipeline?: IrStage[];
}

/** The slice of a pipeline stage the lineage reads. */
interface IrStage {
   type?: string;
   queryFields?: IrQueryField[];
   filterList?: unknown;
   refSummary?: RefSummaryLike;
   /** What the stage returns, by name, with the notes each output carries. */
   outputStruct?: { fields?: IrField[] };
   /** Fields the segment's own `extend:` adds to the source it runs on. */
   extendSource?: IrField[];
}

/** The slice of a `->` query the lineage reads. */
interface IrQuery {
   structRef?: unknown;
   pipeline?: IrStage[];
}

/** A classified field an expression reads, by its path from the source. */
export interface ClassifiedInput {
   field: string;
   classifications: string[];
}

/** One `#(carries)` list a read is held to, and the source that declares it. */
export interface Ceiling {
   source: string;
   classes: string[];
   malformed?: string;
}

/** What the lineage knows about one field of a source. */
export interface FieldClassification {
   /** The ceilings of every carried struct this field lives in below the source or reads through, transitively. */
   ceilings: Ceiling[];
   /** True when some struct on those paths has a chain that could not be read, so its ceiling is unknown. */
   ceilingUnresolved: boolean;
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
   /** Why an annotation on the field is unusable, when one is. */
   malformed?: string;
   /** True for a field under `private:` / `internal:`, which callers never reach. */
   hidden: boolean;
   /** True for a dimension or measure, a record or array among them; false for a view or a join. */
   isField: boolean;
   /** `measure` for an aggregate or analytic expression, else `dimension`. */
   kind: "dimension" | "measure";
   /** Classes the fields it copies or computes from carry that its declared set leaves out. */
   dropped: string[];
   /** Classified fields only its query source's filters read, whose classes every returned row reveals. */
   filtered: ClassifiedInput[];
   /** True for a plain copy of one field (a reference, optionally cast), which cannot de-identify anything. */
   passthrough: boolean;
   /** Paths it reads that resolve to nothing the lineage can see: an unclassified read. */
   unresolved: string[];
   /** True for a query output whose reads Publisher cannot follow, such as one under a nest with stages of its own. */
   unread: boolean;
}

/**
 * What {@link readSourceClassifications} read, by path. `truncated` is true
 * when a join walk stopped at {@link MAX_JOIN_RECURSION_DEPTH} joins or
 * {@link MAX_JOIN_PATHS} join paths: fields past it are absent, not
 * unclassified.
 */
export class SourceLineage extends Map<string, FieldClassification> {
   truncated = false;
   /** On the lineage a later stage runs on: the filters of earlier stages, which every row it reads reveals. */
   rowFilters: ClassifiedInput[] = [];
}

/** The lineage of a compiled source, read once per `fields` array and query: a query source reads differently without its query. */
const lineageMemo = new WeakMap<
   object,
   Map<IrQuery | undefined, SourceLineage>
>();

function activeName(f: IrField): string | undefined {
   return f.as ?? f.name;
}

function isJoinField(f: IrField): boolean {
   return typeof f.join === "string" && Array.isArray(f.fields);
}

function isAtomicField(f: IrField): boolean {
   return !isJoinField(f) && f.type !== "turtle" && !Array.isArray(f.fields);
}

/** A record or array value: join-shaped in the IR, but a dimension a caller selects whole. */
function isRecordField(f: IrField): boolean {
   return (
      (f.type === "record" || f.type === "array") && Array.isArray(f.fields)
   );
}

function fieldKind(f: IrField): "dimension" | "measure" {
   const t = f.expressionType ?? "";
   return t.includes("aggregate") || t.includes("analytic")
      ? "measure"
      : "dimension";
}

/** Whether an expression is one field reference, optionally inside casts or parentheses. */
function isPassthrough(e: unknown): boolean {
   let node = e as { node?: string; type?: string; e?: unknown } | undefined;
   for (let hops = 0; node && hops < 16; hops++) {
      if (node.node === "field" || node.type === "fieldref") return true;
      if (node.node !== "cast" && node.node !== "()") return false;
      node = node.e as typeof node;
   }
   return false;
}

/**
 * Every `{node: "field", path}` or `{type: "fieldref", path}` a tree reads,
 * a read inside an array query re-rooted under its array
 * ({@link forEachFieldPath}).
 */
function fieldReferences(e: unknown): string[][] {
   const out: string[][] = [];
   // Positions are not expressions, and nothing a reference hides in.
   forEachFieldPath(e, (path) => out.push(path), {
      skipKeys: FIELD_REFERENCE_SKIPPED_KEYS,
      fieldrefs: true,
   });
   return out;
}

const FIELD_REFERENCE_SKIPPED_KEYS = new Set(["at", "location"]);

/**
 * The structs a query runs on: the source it names and, for a composite, the
 * member Malloy resolved for it, which the compiler reads before the named one.
 */
function queryRoots(
   modelDef: ModelDef | undefined,
   query: IrQuery | undefined,
): { entry?: IrField; member?: IrField } {
   const holder = { type: "query_source", query } as unknown as SourceDef;
   return {
      entry: resolveQuerySourceBase(holder, modelDef) as IrField | undefined,
      member: resolveCompositeResolvedBase(holder) as IrField | undefined,
   };
}

/**
 * The struct a query's first stage reads: the named (or inline) source, or its
 * resolved composite member, plus the segment's own `extend:`. Undefined when
 * the query names no struct the lineage can read.
 */
function queryInput(
   modelDef: ModelDef | undefined,
   query: IrQuery | undefined,
): { fields: IrField[]; query?: IrQuery } | undefined {
   const { entry, member } = queryRoots(modelDef, query);
   const base = member ?? entry;
   if (!base?.fields) return undefined;
   const extend = query?.pipeline?.[0]?.extendSource;
   return {
      fields: extend?.length ? [...base.fields, ...extend] : base.fields,
      query: base.query,
   };
}

/** The name a query field returns under. */
function queryFieldName(qf: IrQueryField): string | undefined {
   return qf.as ?? qf.name ?? qf.path?.at(-1);
}

/** The paths one query field reads: the field it names, what its expression names, or a nest's summary. */
function queryFieldReads(qf: IrQueryField): string[][] {
   if (qf.type === "fieldref") return [qf.path ?? []];
   if (qf.type === "turtle") {
      return (qf.refSummary?.fieldUsage ?? []).map((u) => [...(u.path ?? [])]);
   }
   return fieldReferences(qf.e);
}

/** The paths a stage reads: the compiler's own summary plus what its fields and filters name. */
function stageReferences(stage: IrStage): {
   refs: ReadonlyArray<ReadonlyArray<string>>;
   summarized: boolean;
   nests: boolean;
} {
   const read = fieldReferences(stage.filterList);
   let nests = false;
   for (const qf of stage.queryFields ?? []) {
      if (qf.type === "turtle") nests = true;
      else read.push(...queryFieldReads(qf));
   }
   const usage = stage.refSummary?.fieldUsage;
   return {
      refs: usage ? [...usage.map((u) => u.path ?? []), ...read] : read,
      summarized: usage !== undefined,
      nests,
   };
}

/**
 * The lineage the stage after `prior` runs on: each output `prior` returns,
 * carrying what it read from `scope` (the lineage `prior` ran on), plus the
 * next stage's own `extend:`, which reads those outputs.
 */
function nextStageLineage(
   scope: SourceLineage,
   prior: IrStage,
   next: IrStage,
): SourceLineage {
   const returned =
      prior.outputStruct?.fields ??
      (prior.queryFields ?? []).flatMap((qf) => {
         const name = queryFieldName(qf);
         return name ? [{ name, annotations: qf.annotations }] : [];
      });
   const lineage = walkSourceClassifications(
      [...returned, ...(next.extendSource ?? [])],
      stageOutputs(scope, prior),
   );
   const seen = new Set<string>();
   for (const f of returned) {
      const name = activeName(f);
      for (const c of (name ? lineage.get(name)?.filtered : undefined) ?? []) {
         if (seen.has(c.field)) continue;
         seen.add(c.field);
         lineage.rowFilters.push(c);
      }
   }
   return lineage;
}

/**
 * What a compiled query reads, stage by stage: each stage's own references
 * (`refSummary.fieldUsage`, filters included, plus what its fields name) and
 * the lineage of what they resolve against. The first stage resolves against
 * {@link queryInput}. A later stage reads the stage before it, which was
 * already held to account, so only what its own `extend:` brings in is added
 * (a join to a source adds that source's fields), resolved against the
 * earlier stage's outputs carrying what they read ({@link nextStageLineage}).
 * A stage Malloy gave no summary (one that reads no field) is read from its
 * query fields and filters instead, and `complete` is false when that stage
 * nests, which that read cannot follow. An `extend:` name that is already in
 * the lineage leaves `complete` false only when either meaning carries a class
 * or cannot be resolved; two plain ones judge the same. `scopes` holds the
 * lineage each stage's own names resolve against. Undefined when the query
 * names no struct the lineage can read.
 */
export function readQueryFieldReads(
   modelDef: ModelDef | undefined,
   query: unknown,
):
   | {
        lineage: SourceLineage;
        fields: IrField[];
        paths: string[];
        scopes: SourceLineage[];
        complete: boolean;
     }
   | undefined {
   const q = query as IrQuery | undefined;
   const stages = q?.pipeline;
   if (!stages?.length) return undefined;
   const input = queryInput(modelDef, q);
   if (!input) return undefined;
   const first = readSourceClassifications(input.fields, input.query);
   let lineage = first;
   let complete = true;
   const paths = new Set<string>();
   const keep = (refs: ReadonlyArray<ReadonlyArray<string>>): void => {
      // An empty path is the source itself (a `count()`), not a field.
      for (const r of refs) if (r.length > 0) paths.add(r.join("."));
   };
   let scope = first;
   const scopes: SourceLineage[] = [first];
   stages.forEach((stage, i) => {
      // An `index:` reads every field its wildcard names, which no reference lists.
      if (stage.type === "index") complete = false;
      const { refs, summarized, nests } = stageReferences(stage);
      if (!summarized && nests) complete = false;
      if (i === 0) {
         const outputs = new Set(
            (stage.queryFields ?? []).flatMap((qf) => queryFieldName(qf) ?? []),
         );
         // A bare name the stage itself returns (`order_by: n`) is not a field of the
         // source; a dotted path is, even when its head shadows an output name.
         keep(
            refs.filter(
               (r) =>
                  first.has(r.join(".")) ||
                  !(r.length === 1 && outputs.has(r[0])),
            ),
         );
         return;
      }
      // A later stage reads the one before it, already held to account, so
      // only what its own `extend:` brings in is a read of the source.
      scope = nextStageLineage(scope, stages[i - 1], stage);
      scopes[i] = scope;
      const extend = stage.extendSource ?? [];
      if (extend.length === 0) return;
      const names = new Set(extend.flatMap((f) => activeName(f) ?? []));
      if (lineage === first) {
         lineage = new SourceLineage(first);
         lineage.truncated = first.truncated;
      }
      for (const [path, c] of scope) {
         if (!names.has(path.split(".")[0])) continue;
         const held = lineage.get(path);
         if (!held) lineage.set(path, c);
         else if (!sameJudgment(held, c)) complete = false;
      }
      if (scope.truncated) lineage.truncated = true;
      keep(refs.filter((r) => names.has(r[0])));
   });
   return {
      lineage,
      fields: input.fields,
      paths: Array.from(paths),
      scopes,
      complete,
   };
}

/** Whether two meanings of one path are plain alike: no class, ceiling or unread input on either, so either judges the same. */
function sameJudgment(a: FieldClassification, b: FieldClassification): boolean {
   const plain = (c: FieldClassification): boolean =>
      c.ceilings.length === 0 &&
      !c.ceilingUnresolved &&
      c.declared.length === 0 &&
      c.classifications.length === 0 &&
      c.dropped.length === 0 &&
      c.filtered.length === 0 &&
      c.unresolved.length === 0 &&
      !c.unread &&
      !c.malformed;
   return plain(a) && plain(b) && a.isField === b.isField;
}

/**
 * Whether any struct the compiled model reaches declares a `#(carries)`: its
 * sources and whatever they join, compose, run on or extend, an imported one
 * included, since `contents` holds only the names a file imports. A caller
 * cannot add one. The cheap pre-check before a query is read; read once per
 * compiled model.
 */
export function modelDeclaresCarries(modelDef: ModelDef | undefined): boolean {
   if (!modelDef) return false;
   let declared = carriesMemo.get(modelDef);
   if (declared === undefined) {
      declared = walkForCarries(
         Object.values(modelDef.contents ?? {}),
         modelDef,
         false,
      );
      carriesMemo.set(modelDef, declared);
   }
   return declared;
}

const carriesMemo = new WeakMap<object, boolean>();

/** Per root struct, then per model def, since a name resolves against the model def it is read in. */
const reachMemo = new WeakMap<object, WeakMap<object, boolean>>();

/**
 * Whether a `#(carries)` sits anywhere under `roots`: the structs a query runs
 * on, joins or extends. A name that resolves to nothing is unknown and counts
 * as one, in a model that declares any, so what Publisher cannot see stays
 * refused; a model that declares none has nothing for a query to reach.
 */
export function reachesCarries(
   roots: ReadonlyArray<unknown>,
   modelDef: ModelDef | undefined,
): boolean {
   if (!modelDef || !modelDeclaresCarries(modelDef)) return false;
   return roots.some((root) => {
      if (!root || typeof root !== "object") return false;
      let byModel = reachMemo.get(root);
      if (!byModel) reachMemo.set(root, (byModel = new WeakMap()));
      let reaches = byModel.get(modelDef);
      if (reaches === undefined) {
         reaches = walkForCarries([root], modelDef, true);
         byModel.set(modelDef, reaches);
      }
      return reaches;
   });
}

/** A `query.structRef` name is followed through `contents`; one that resolves to nothing is `unknownReaches`. */
function walkForCarries(
   roots: unknown[],
   modelDef: ModelDef,
   unknownReaches: boolean,
): boolean {
   const seen = new Set<object>();
   const stack = [...roots];
   while (stack.length > 0) {
      const node = stack.pop();
      if (!node || typeof node !== "object" || seen.has(node)) continue;
      seen.add(node);
      if (Array.isArray(node)) {
         stack.push(...node);
         continue;
      }
      const obj = node as Record<string, unknown>;
      for (const key of ["notes", "blockNotes"]) {
         const notes = obj[key];
         if (
            Array.isArray(notes) &&
            notes.some((n) =>
               isOnRoute(String((n as { text?: unknown })?.text), "carries"),
            )
         ) {
            return true;
         }
      }
      for (const [key, value] of Object.entries(obj)) {
         // Expressions and positions hold no struct.
         if (key === "e" || key === "at" || key === "location") continue;
         if (key === "structRef" && typeof value === "string") {
            const base = modelDef.contents?.[value];
            if (base) stack.push(base);
            else if (unknownReaches) return true;
         } else if (value && typeof value === "object") {
            stack.push(value);
         }
      }
   }
   return false;
}

/**
 * The ceilings a struct carries: its own, read down its annotation chain so a
 * source declared over a carried one has it, and, for a `->` query source,
 * those of the struct its query runs on (and of its resolved composite
 * member), recursively. `member` is the composite member a query resolved for
 * `entry`, which a source alone does not name. `unresolved` when a base cannot
 * be found, which leaves its ceiling unknown.
 */
export function structCeilings(
   modelDef: ModelDef | undefined,
   entry: IrField | undefined,
   name: string,
   member?: IrField,
): { ceilings: Ceiling[]; unresolved: boolean } {
   const ceilings: Ceiling[] = [];
   let unresolved = false;
   let budget = MAX_JOIN_RECURSION_DEPTH;
   const climb = (start: IrField | undefined, startLabel: string): void => {
      let node = start;
      let label = startLabel;
      while (node) {
         if (budget-- <= 0) {
            unresolved = true;
            return;
         }
         const carries = readCarries(node.annotations);
         if (carries.declared) {
            ceilings.push({
               source: label,
               classes: Array.from(carries.classes).sort(byCodepoint),
               ...(carries.malformed ? { malformed: carries.malformed } : {}),
            });
         }
         const asStruct = node as unknown as SourceDef;
         const resolved = resolveCompositeResolvedBase(asStruct) as
            | IrField
            | undefined;
         if (resolved) climb(resolved, resolved.as ?? resolved.name ?? label);
         if (!node.query?.structRef) return;
         const base = resolveQuerySourceBase(asStruct, modelDef) as
            | IrField
            | undefined;
         if (!base) {
            unresolved = true;
            return;
         }
         label = base.as ?? base.name ?? label;
         node = base;
      }
   };
   climb(entry, name);
   if (member) climb(member, member.as ?? member.name ?? name);
   return { ceilings, unresolved };
}

/** The two ceiling lists joined, each distinct one once. */
function mergeCeilings(a: Ceiling[], b: Ceiling[]): Ceiling[] {
   if (b.length === 0) return a;
   const key = (c: Ceiling) => `${c.source}|${c.classes.join(",")}`;
   const seen = new Set(a.map(key));
   const extra = b.filter((c) => !seen.has(key(c)));
   return extra.length > 0 ? [...a, ...extra] : a;
}

/** The strictest of a struct's ceilings: the classes on every list. */
export function strictestCarries(
   ceilings: readonly Ceiling[],
   unresolved: boolean,
   entry: IrField,
   modelDef: ModelDef | undefined,
): { declared: boolean; classes: Set<string>; malformed?: string } {
   const malformed =
      ceilings.find((c) => c.malformed)?.malformed ??
      (unresolved && (ceilings.length > 0 || reachesCarries([entry], modelDef))
         ? "runs on a source Publisher cannot read, so what its #(carries) holds it to is unknown"
         : undefined);
   const lists = ceilings.map((c) => new Set(c.classes));
   const classes = new Set(
      lists.length > 0
         ? [...lists[0]].filter((k) => lists.every((l) => l.has(k)))
         : [],
   );
   return {
      declared: lists.length > 0 || malformed !== undefined,
      classes,
      ...(malformed ? { malformed } : {}),
   };
}

/**
 * Why a query reads past a `#(carries)` ceiling, or undefined when every field
 * it reads is classified on every list that applies. Those are the lists of
 * the struct it enters (and of the struct a `->` source runs on) and of each
 * carried join a read passes through. The `on` of each caller join counts as a
 * read, which the compiled query does not list. A read Publisher cannot place
 * or follow refuses, as does a walk cut short.
 */
export function ceilingRefusal(
   modelDef: ModelDef | undefined,
   query: unknown,
   callerJoins: readonly unknown[] = [],
): string | undefined {
   const ref = (query as IrQuery | undefined)?.structRef;
   const { entry, member } = queryRoots(modelDef, query as IrQuery);
   const own = structCeilings(
      modelDef,
      entry,
      typeof ref === "string" ? ref : (entry?.as ?? entry?.name ?? ""),
      member,
   );
   // A query that reaches no #(carries) has no ceiling to hold a read to, whatever else the model carries.
   if (
      own.ceilings.length === 0 &&
      !reachesCarries([entry, member, query, ...callerJoins], modelDef)
   ) {
      return undefined;
   }
   const headOf = (c: Ceiling): string =>
      `source ${c.source} carries [${c.classes.join(", ")}]; this query reads`;
   if (own.unresolved) {
      return `source ${typeof ref === "string" ? ref : (entry?.as ?? entry?.name ?? "")} runs on a source which Publisher cannot see completely`;
   }
   const reads = readQueryFieldReads(modelDef, query);
   if (!reads) {
      return own.ceilings.length > 0
         ? `${headOf(own.ceilings[0])} a struct which Publisher cannot see completely`
         : undefined;
   }
   // A caller join's `on` reads the scope of the stage that declares it.
   const stages = (query as IrQuery | undefined)?.pipeline ?? [];
   const lookups = new Map<string, { path: string; lineage: SourceLineage }>();
   const look = (path: string, lineage: SourceLineage, at: number): void => {
      lookups.set(`${at}|${path}`, { path, lineage });
   };
   for (const path of reads.paths) look(path, reads.lineage, 0);
   for (const join of callerJoins) {
      const at = stages.findIndex((stage) =>
         stage.extendSource?.includes(join as IrField),
      );
      const lineage = (at > 0 ? reads.scopes[at] : undefined) ?? reads.lineage;
      for (const r of fieldReferences(
         (join as { onExpression?: unknown }).onExpression,
      )) {
         if (r.length > 0) look(r.join("."), lineage, Math.max(at, 0));
      }
   }
   const reading = Array.from(lookups.values()).sort((x, y) =>
      byCodepoint(x.path, y.path),
   );
   if (!reads.complete || reads.lineage.truncated) {
      const first = own.ceilings[0] ?? {
         source: "a joined source",
         classes: [],
      };
      return `${headOf(first)} ${reading[0]?.path ?? first.source}, which Publisher cannot see completely`;
   }
   for (const { path, lineage } of reading) {
      const known = lineage.get(path);
      const applicable = mergeCeilings(own.ceilings, known?.ceilings ?? []);
      const unseen = (): string =>
         `${headOf(applicable[0] ?? { source: "a joined source", classes: [] })} ${path}, which Publisher cannot see completely`;
      if (!known) return unseen();
      // A view or a join is not a field; what it reads is listed on its own.
      if (!known.isField) continue;
      if (
         (known.unresolved.length > 0 && known.declared.length === 0) ||
         known.ceilingUnresolved
      ) {
         return unseen();
      }
      const valueClasses = [
         ...known.classifications,
         // A copy declared below its input still returns the input.
         ...(known.passthrough ? known.dropped : []),
      ];
      const filterClasses = known.filtered.flatMap((i) => i.classifications);
      for (const c of applicable) {
         if (valueClasses.length === 0 && filterClasses.length === 0) {
            return `${headOf(c)} ${path}, which has no #(classification) on the list`;
         }
         const off = valueClasses.filter((k) => !c.classes.includes(k));
         if (off.length > 0) {
            return `${headOf(c)} ${path}, which is classified ${off.join(" ")}, not on the list`;
         }
         // A field declared on the list can still reveal what its rows were filtered on.
         const filters = known.filtered.filter((i) =>
            i.classifications.some((k) => !c.classes.includes(k)),
         );
         if (filters.length > 0) {
            const named = filters
               .map((i) => `${i.field} (${i.classifications.join(" ")})`)
               .join(", ");
            return `${headOf(c)} ${path}, which filters on ${named}, not on the list`;
         }
      }
   }
   return undefined;
}

/** What one output field of a `->` query source reads. */
interface OutputReads {
   value: string[][];
   filter: string[][];
   passthrough: boolean;
   /** True for a `nest:`, whose outputs are each read on their own. */
   nest?: boolean;
}

/**
 * The classification of every field reachable from a source, keyed by its
 * path from the source (`ssn`, `patient.ssn`), joins followed
 * {@link MAX_JOIN_RECURSION_DEPTH} deep and {@link MAX_JOIN_PATHS} wide, and
 * memoized on the compiled source's `fields`. Hidden fields are in the map, because a
 * public field may be derived from one; `hidden` says so, and a field under a
 * hidden join is hidden. A reference an expression makes is resolved relative
 * to the struct the field lives in, so `patient.full_name is concat(first,
 * last)` reads `patient.first`; one that resolves to nothing is an
 * `unresolved` read, never a skipped one.
 *
 * For a `->` query source, pass its `query`: each output field then reads
 * what its query field reads, plus the stage's filters, resolved through
 * every stage to the source the query runs on.
 */
export function readSourceClassifications(
   fields: IrField[] | undefined,
   query?: IrQuery,
): SourceLineage {
   const byQuery = fields ? lineageMemo.get(fields) : undefined;
   const memoized = byQuery?.get(query);
   if (memoized) return memoized;
   const lineage = walkSourceClassifications(
      fields,
      query ? readQueryOutputs(query) : undefined,
   );
   if (fields) {
      if (byQuery) byQuery.set(query, lineage);
      else lineageMemo.set(fields, new Map([[query, lineage]]));
   }
   return lineage;
}

/**
 * Whether a record or array has one value its parts are pieces of: built by an
 * expression, a part of such a value, or a query output read as one field (a
 * nest's outputs are each read on their own).
 */
function computesValue(
   field: IrField,
   out: OutputScope | undefined,
   record: string | undefined,
): boolean {
   if (!isRecordField(field)) return false;
   if (field.e !== undefined || record !== undefined) return true;
   const reads = out?.outputs.reads.get(out.rel);
   return reads !== undefined && !reads.nest;
}

/**
 * The element expression of a part of a record literal (`r is { a is x }`),
 * with the owner the record's expression reads from, or undefined for any
 * other field: the record's IR keeps each element's expression in its
 * literal's `kids` by name, so a part reads what its own element reads
 * rather than the whole record's value.
 */
function recordLiteralPart(
   entry: { field: IrField; record?: string },
   byPath: ReadonlyMap<string, { field: IrField; owner: string }>,
): { e: unknown; owner: string } | undefined {
   if (entry.record === undefined) return undefined;
   const record = byPath.get(entry.record);
   const e = record?.field.e as
      | { node?: unknown; kids?: Record<string, unknown> }
      | undefined;
   if (!record || e?.node !== "recordLiteral" || !e.kids) return undefined;
   const name = activeName(entry.field);
   if (!name || !Object.prototype.hasOwnProperty.call(e.kids, name)) {
      return undefined;
   }
   return { e: e.kids[name], owner: record.owner };
}

function walkSourceClassifications(
   fields: IrField[] | undefined,
   outputs: QueryOutputs | undefined,
): SourceLineage {
   const resolved = new SourceLineage();
   if (outputs?.base?.truncated) resolved.truncated = true;
   let joinPaths = 0;
   const byPath = new Map<
      string,
      {
         field: IrField;
         owner: string;
         hidden: boolean;
         /** Set when the field is an output of a `->` query, not something declared over it. */
         out?: OutputScope;
         /** The carried structs on the join path to it. */
         ceilings: Ceiling[];
         ceilingUnresolved: boolean;
         /** The computed record or array whose value this field is a part of. */
         record?: string;
      }
   >();
   const outputsOf = (q: IrQuery): QueryOutputs => {
      const outputs = readQueryOutputs(q);
      if (outputs.base?.truncated) resolved.truncated = true;
      return outputs;
   };
   const collect = (
      list: IrField[],
      owner: string,
      joins: number,
      hidden: boolean,
      scope: OutputScope | undefined,
      ceilings: Ceiling[],
      ceilingUnresolved: boolean,
      record: string | undefined,
   ): void => {
      for (const field of list) {
         const name = activeName(field);
         if (!name) continue;
         const path = owner ? `${owner}.${name}` : name;
         const isHidden = hidden || field.accessModifier !== undefined;
         // A query source's own `extend:` fields are declared over its outputs, not outputs themselves.
         const isOutput =
            scope !== undefined &&
            (scope.rel !== "" ||
               (scope.outputs.names
                  ? scope.outputs.names.has(name)
                  : field.e === undefined &&
                    field.type !== "turtle" &&
                    !isJoinField(field)));
         const out =
            isOutput && scope
               ? {
                    outputs: scope.outputs,
                    rel: scope.rel ? `${scope.rel}.${name}` : name,
                 }
               : undefined;
         byPath.set(path, {
            field,
            owner,
            hidden: isHidden,
            out,
            ceilings,
            ceilingUnresolved,
            record,
         });
         if (!isJoinField(field)) continue;
         if (
            joins >= MAX_JOIN_RECURSION_DEPTH ||
            ++joinPaths > MAX_JOIN_PATHS
         ) {
            resolved.truncated = true;
            continue;
         }
         const inner =
            out ??
            (field.query
               ? { outputs: outputsOf(field.query), rel: "" }
               : undefined);
         const through = structCeilings(undefined, field, path);
         collect(
            field.fields ?? [],
            path,
            joins + 1,
            isHidden,
            inner,
            mergeCeilings(ceilings, through.ceilings),
            ceilingUnresolved || through.unresolved,
            computesValue(field, out, record) ? path : undefined,
         );
      }
   };
   collect(
      fields ?? [],
      "",
      0,
      false,
      outputs ? { outputs, rel: "" } : undefined,
      [],
      false,
      undefined,
   );

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
      const { field, owner, hidden, out } = entry;
      let ceilings = entry.ceilings;
      let ceilingUnresolved = entry.ceilingUnresolved;
      const texts = annotationTexts(field.annotations);
      const own = readClassification(texts);
      const outReads = out ? out.outputs.reads.get(out.rel) : undefined;
      // A part of a computed record carries no expression of its own: it returns a piece of the record.
      // A part of a record literal (`r is { a is x, b is y }`) is its own element, read beside the record.
      const literal = outReads ? undefined : recordLiteralPart(entry, byPath);
      const partOf = outReads || literal ? undefined : entry.record;
      // A record with no one value, such as a struct column or a nest, returns its parts.
      const parts =
         isRecordField(field) && !computesValue(field, out, entry.record)
            ? (field.fields ?? []).flatMap((f) => {
                 const name = activeName(f);
                 return name ? [`${path}.${name}`] : [];
              })
            : [];
      // An output of a query reads from the query's source, not from its own siblings.
      const reads: OutputReads = outReads?.nest
         ? // A nest's own summary names what its extend declares; its outputs carry what they read.
           { value: [], filter: [], passthrough: false }
         : (outReads ?? {
              value:
                 out || partOf ? [] : fieldReferences(literal?.e ?? field.e),
              filter: [],
              // An output whose reads are unknown may be a copy.
              passthrough:
                 out || partOf ? true : isPassthrough(literal?.e ?? field.e),
           });
      const inputs: ClassifiedInput[] = [];
      const seenInput = new Set<string>();
      const unresolved: string[] = [];
      const unresolvedRead = (refPath: string): void => {
         if (!unresolved.includes(refPath)) unresolved.push(refPath);
      };
      const unread = out !== undefined && !outReads && !partOf;
      if (unread) unresolvedRead(path);
      // A nest is charged through its parts; one with no readable parts has no known reads.
      if (outReads?.nest && parts.length === 0) unresolvedRead(path);
      const take = (list: ClassifiedInput[]): void => {
         for (const c of list) {
            if (seenInput.has(c.field)) continue;
            seenInput.add(c.field);
            inputs.push(c);
         }
      };
      const filters: ClassifiedInput[] = [];
      const read = (refs: string[][]): void => {
         for (const ref of refs) {
            const name = ref.join(".");
            const from = literal ? literal.owner : owner;
            const refPath = from && !out ? `${from}.${name}` : name;
            if (refPath === path && !out) continue;
            readInput(
               refPath,
               out ? out.outputs.base?.get(refPath) : resolve(refPath),
            );
         }
      };
      const readInput = (
         refPath: string,
         input: FieldClassification | undefined,
      ): void => {
         if (!input) {
            unresolvedRead(refPath);
            return;
         }
         // A read inherits the ceilings of whatever it reads through.
         ceilings = mergeCeilings(ceilings, input.ceilings);
         ceilingUnresolved ||= input.ceilingUnresolved;
         if (input.unresolved.length > 0 && input.declared.length === 0) {
            unresolvedRead(refPath);
         }
         // A classified input is an input; an unclassified one contributes
         // the inputs IT was read from, so lineage crosses an intermediate.
         take(
            input.classifications.length > 0
               ? [
                    {
                       field: refPath,
                       // A relabel declared below its input still passes the input's classes on.
                       classifications: input.passthrough
                          ? tokenSet([
                               ...input.classifications,
                               ...input.dropped,
                            ])
                          : input.classifications,
                    },
                 ]
               : input.inputs,
         );
         // Every row of a filtered output reveals what its filter selects on, a filter here too, not a value.
         filters.push(...input.filtered);
      };
      read(reads.value);
      for (const p of partOf ? [partOf] : parts) readInput(p, resolve(p));
      const valueInputs = inputs.length;
      read(reads.filter);
      take(filters);
      take(out?.outputs.rowFilters ?? []);
      visiting.delete(path);
      const classesOf = (list: ClassifiedInput[]): string[] =>
         tokenSet(list.flatMap((i) => i.classifications));
      const wouldBe = classesOf(inputs);
      const result: FieldClassification = {
         declared: own.values,
         inferred: own.values.length > 0 ? [] : wouldBe,
         classifications: own.values.length > 0 ? own.values : wouldBe,
         inputs,
         ...(own.malformed ? { malformed: own.malformed } : {}),
         hidden,
         isField: isAtomicField(field) || isRecordField(field),
         kind: fieldKind(field),
         // A filter selects rows and copies no value, so only the value reads are "dropped".
         dropped:
            own.values.length > 0
               ? classesOf(inputs.slice(0, valueInputs)).filter(
                    (k) => !own.values.includes(k),
                 )
               : [],
         filtered: inputs.slice(valueInputs),
         passthrough: reads.passthrough,
         unresolved,
         unread,
         ceilings,
         ceilingUnresolved,
      };
      resolved.set(path, result);
      return result;
   };
   for (const path of byPath.keys()) resolve(path);
   return resolved;
}

/** What the outputs of one `->` query read, and the lineage they read it from. */
interface QueryOutputs {
   base: SourceLineage | undefined;
   /** By output path: a nest's outputs are `nest.name`. */
   reads: Map<string, OutputReads>;
   /** The names its last stage returns; undefined when the compiled query does not say. */
   names: Set<string> | undefined;
   /** The filters of the stages before its last, which every output reveals. */
   rowFilters: ClassifiedInput[];
}

/**
 * What each output of a `->` query reads, resolved through every stage: the
 * first stage against the lineage of {@link queryInput}, each later one
 * against the outputs of the stage before it ({@link nextStageLineage}).
 * An output of a nest that cannot be read is absent and the caller treats it
 * as unresolved.
 */
function readQueryOutputs(query: IrQuery): QueryOutputs {
   const input = queryInput(undefined, query);
   const stages = query.pipeline ?? [];
   let base = input
      ? readSourceClassifications(input.fields, input.query)
      : undefined;
   for (let i = 1; base && i < stages.length; i++) {
      base = nextStageLineage(base, stages[i - 1], stages[i]);
   }
   const last = stages.at(-1);
   return last
      ? stageOutputs(base, last)
      : { base, reads: new Map(), names: undefined, rowFilters: [] };
}

/**
 * What each output of one stage reads from `base`, the lineage the stage runs
 * on: its query field's references and the filters of the stage it sits in
 * and of every stage around it. A nest's outputs are read the same way, one
 * single-stage level at a time.
 */
function stageOutputs(
   base: SourceLineage | undefined,
   stage: IrStage,
): QueryOutputs {
   const reads = new Map<string, OutputReads>();
   const collectStage = (
      level: IrStage,
      prefix: string,
      inherited: string[][],
      depth: number,
   ): void => {
      const filter = [...inherited, ...fieldReferences(level.filterList)];
      for (const qf of level.queryFields ?? []) {
         const name = queryFieldName(qf);
         if (!name) continue;
         const path = prefix ? `${prefix}.${name}` : name;
         reads.set(path, {
            value: queryFieldReads(qf),
            filter,
            passthrough: qf.type === "fieldref" || isPassthrough(qf.e),
            ...(qf.type === "turtle" ? { nest: true } : {}),
         });
         const nested = qf.type === "turtle" ? qf.pipeline : undefined;
         if (nested?.length === 1 && depth < MAX_JOIN_RECURSION_DEPTH) {
            collectStage(nested[0], path, filter, depth + 1);
         }
      }
   };
   collectStage(stage, "", [], 0);
   const returned = stage.outputStruct?.fields;
   return {
      base,
      reads,
      names: returned
         ? new Set(returned.flatMap((f) => activeName(f) ?? []))
         : undefined,
      rowFilters: base?.rowFilters ?? [],
   };
}

/** Where in a query's outputs a field sits: the query, and its path among them. */
interface OutputScope {
   outputs: QueryOutputs;
   rel: string;
}

/** What a view reads from its source. */
export interface ViewExposure {
   name: string;
   /** The union of the classes of the fields it selects, aggregates and filters on. */
   classifications: string[];
   reads: ClassifiedInput[];
   /** Paths it reads that carry no class, or resolve to nothing on the source. */
   unclassified: string[];
   /** True when the compiled view has no reference summary, so what it reads is unknown. */
   unread: boolean;
}

/**
 * The classes each top-level view reads, from its first stage's own
 * references (`turtle.refSummary ?? pipeline[0].refSummary`): directly, not
 * through a join's ON clause, so a view that groups by `patient.*` is not
 * charged for the key it joins on. Hidden fields count, because a view
 * returns what it reads.
 */
export function readViewExposures(
   fields: IrField[] | undefined,
   lineage: Map<string, FieldClassification>,
): ViewExposure[] {
   const out: ViewExposure[] = [];
   for (const f of fields ?? []) {
      if (f.type !== "turtle") continue;
      const name = activeName(f);
      if (!name) continue;
      const summary = f.refSummary ?? f.pipeline?.[0]?.refSummary;
      const reads: ClassifiedInput[] = [];
      const unclassified: string[] = [];
      for (const usage of summary?.fieldUsage ?? []) {
         const path = (usage.path ?? []).join(".");
         const c = lineage.get(path);
         if (c && !c.isField) continue;
         if (
            !c ||
            c.classifications.length === 0 ||
            (c.unresolved.length > 0 && c.declared.length === 0)
         ) {
            if (!unclassified.includes(path)) unclassified.push(path);
         }
         if (c && c.classifications.length > 0) {
            if (!reads.some((r) => r.field === path)) {
               // A copy declared below its input is still its input.
               reads.push({
                  field: path,
                  classifications: c.passthrough
                     ? tokenSet([...c.classifications, ...c.dropped])
                     : c.classifications,
               });
            }
            for (const f of c.filtered) {
               if (!reads.some((r) => r.field === f.field)) reads.push(f);
            }
         }
      }
      out.push({
         name,
         classifications: tokenSet(reads.flatMap((r) => r.classifications)),
         reads,
         unclassified,
         unread: summary === undefined,
      });
   }
   return out;
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

/** Whether one gate expression denies every caller. */
export const isDenyAll = (expr: string) =>
   expr.trim().toLowerCase() === "false";

/** Whether a compiled source is denied to every caller, so nothing it publishes is reachable. */
export function isDeniedToAll(apiSource: {
   accessFilter?: string[];
   authorize?: string[];
}): boolean {
   return (
      (apiSource.accessFilter ?? []).some(isDenyAll) ||
      (apiSource.authorize ?? []).some(isDenyAll)
   );
}
