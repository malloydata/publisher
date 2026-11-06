// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(classification) "<class>"` on a field and `#(carries) ["<class>", …]` on a
 * source: what a field is and what an entry point may expose, both in the
 * words of the package's access policy. Values are
 * quoted and lists are bracketed and comma-separated, read by Malloy's tag
 * parser (`readTagList`). Publisher reads the shape and never
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
 *   once ANY field in the package carries a class
 *   ({@link classificationWarnings}). Self-declaring: a package that
 *   classifies nothing is not warned about. A malformed annotation warns
 *   `classification-malformed` naming the field and the fix.
 * - **Ceiling.** `#(carries)` on a source lists the classes a caller may
 *   reach from it. A field reachable from that source (own, inherited,
 *   inferred, or through a join) with any class off the list, or no class at
 *   all, is a leak, and a leak refuses the package load
 *   ({@link classificationLeaks}), the way a gate that does not parse does.
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
import { parseAnnotation } from "@malloydata/malloy-tag";
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
} from "./gate_registry_walk";
import type { Model } from "./model";

type ApiPackageWarning = NonNullable<
   components["schemas"]["Package"]["warnings"]
>[number];

/** `PackageWarning.code` of a published source with unclassified fields. */
export const CLASSIFICATION_COVERAGE = "classification-coverage";
/** `PackageWarning.code` of a `#(classification)` or `#(carries)` annotation that names nothing usable. */
export const CLASSIFICATION_MALFORMED = "classification-malformed";
/** `PackageWarning.code` of a computed field that declares a lower class than the fields it reads. */
export const CLASSIFICATION_DOWNGRADE = "classification-downgrade";

const CLASSIFICATION_LINE = /^#\(classification\)(.*)$/s;
const CARRIES_LINE = /^#\(carries\)(.*)$/s;

/** One annotation note, as `to_stable` (`{value}`) or a raw string carries it. */
type AnnotationText = string | { value: string };

function annotationText(a: AnnotationText): string {
   return typeof a === "string" ? a : a.value;
}

/** Whether an annotation line is a `#(classification)` or `#(carries)` note. */
export function isClassificationLine(text: string): boolean {
   const t = text.trim();
   return CLASSIFICATION_LINE.test(t) || CARRIES_LINE.test(t);
}

/**
 * Whether anything in the package declares a `#(carries)` or a
 * `#(classification)`: the opt-in to the checks that refuse a load. Reads every
 * compiled source, hidden ones too, and every field under it.
 */
export function packageUsesClassification(
   models: ReadonlyMap<string, Model>,
): boolean {
   const seen = new Set<object>();
   const declares = (node: IrField | undefined, depth: number): boolean => {
      if (!node || seen.has(node) || depth > MAX_JOIN_RECURSION_DEPTH + 1) {
         return false;
      }
      seen.add(node);
      if (
         (annotationTexts(node.annotations) ?? []).some(isClassificationLine)
      ) {
         return true;
      }
      return (node.fields ?? []).some((f) => declares(f, depth + 1));
   };
   for (const model of models.values()) {
      let modelDef: ModelDef | undefined;
      try {
         modelDef = model.getModelDef();
      } catch {
         continue;
      }
      for (const def of Object.values(modelDef?.contents ?? {})) {
         if (declares(def as IrField, 0)) return true;
      }
   }
   return false;
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
   /** True for a dimension or measure; false for a view or a join. */
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
 * nests, which that read cannot follow. Undefined when the query names no
 * struct the lineage can read.
 */
export function readQueryFieldReads(
   modelDef: ModelDef | undefined,
   query: unknown,
):
   | {
        lineage: SourceLineage;
        fields: IrField[];
        paths: string[];
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
      const extend = stage.extendSource ?? [];
      if (extend.length === 0) return;
      const names = new Set(extend.flatMap((f) => activeName(f) ?? []));
      if (lineage === first) {
         lineage = new SourceLineage(first);
         lineage.truncated = first.truncated;
      }
      for (const [path, c] of scope) {
         if (!names.has(path.split(".")[0])) continue;
         if (lineage.has(path)) complete = false;
         else lineage.set(path, c);
      }
      if (scope.truncated) lineage.truncated = true;
      keep(refs.filter((r) => names.has(r[0])));
   });
   return {
      lineage,
      fields: input.fields,
      paths: Array.from(paths),
      complete,
   };
}

/**
 * Whether any struct the compiled model reaches declares a `#(carries)`: its
 * sources and whatever they join, compose, run on or extend, an imported one
 * included, since `contents` holds only the names a file imports. A caller
 * cannot add one. Read once per compiled model.
 */
export function modelDeclaresCarries(modelDef: ModelDef | undefined): boolean {
   if (!modelDef) return false;
   let declared = carriesMemo.get(modelDef);
   if (declared === undefined) {
      declared = reachesCarries(Object.values(modelDef.contents ?? {}));
      carriesMemo.set(modelDef, declared);
   }
   return declared;
}

const carriesMemo = new WeakMap<object, boolean>();

/** Whether a note on the `#(carries)` route sits anywhere under `roots`. */
function reachesCarries(roots: unknown[]): boolean {
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
               CARRIES_LINE.test(
                  String((n as { text?: unknown })?.text).trim(),
               ),
            )
         ) {
            return true;
         }
      }
      for (const [key, value] of Object.entries(obj)) {
         // Expressions and positions hold no struct.
         if (key === "e" || key === "at" || key === "location") continue;
         if (value && typeof value === "object") stack.push(value);
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
function structCeilings(
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
function strictestCarries(
   ceilings: readonly Ceiling[],
   unresolved: boolean,
   modelDef: ModelDef | undefined,
): { declared: boolean; classes: Set<string>; malformed?: string } {
   const malformed =
      ceilings.find((c) => c.malformed)?.malformed ??
      (unresolved && (ceilings.length > 0 || modelDeclaresCarries(modelDef))
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
   declaresCarries = modelDeclaresCarries(modelDef),
): string | undefined {
   // A model that declares no #(carries) has nothing to hold a read to.
   if (!declaresCarries) return undefined;
   const ref = (query as IrQuery | undefined)?.structRef;
   const { entry, member } = queryRoots(modelDef, query as IrQuery);
   const own = structCeilings(
      modelDef,
      entry,
      typeof ref === "string" ? ref : (entry?.as ?? entry?.name ?? ""),
      member,
   );
   const headOf = (c: Ceiling): string =>
      `source ${c.source} carries [${c.classes.join(", ")}]; this query reads`;
   if (own.unresolved && (own.ceilings.length > 0 || declaresCarries)) {
      return `source ${typeof ref === "string" ? ref : (entry?.as ?? entry?.name ?? "")} runs on a source which Publisher cannot see completely`;
   }
   const reads = readQueryFieldReads(modelDef, query);
   if (!reads) {
      return own.ceilings.length > 0
         ? `${headOf(own.ceilings[0])} a struct which Publisher cannot see completely`
         : undefined;
   }
   const paths = Array.from(
      new Set([
         ...reads.paths,
         ...callerJoins.flatMap((j) =>
            fieldReferences((j as { onExpression?: unknown }).onExpression)
               .filter((r) => r.length > 0)
               .map((r) => r.join(".")),
         ),
      ]),
   ).sort(byCodepoint);
   if (!reads.complete || reads.lineage.truncated) {
      const first = own.ceilings[0] ?? {
         source: "a joined source",
         classes: [],
      };
      return `${headOf(first)} ${paths[0] ?? first.source}, which Publisher cannot see completely`;
   }
   for (const path of paths) {
      const known = reads.lineage.get(path);
      const applicable = mergeCeilings(own.ceilings, known?.ceilings ?? []);
      const unseen = (): string =>
         `${headOf(applicable[0] ?? { source: "a joined source", classes: [] })} ${path}, which Publisher cannot see completely`;
      if (!known) return unseen();
      // A view or a join is not a field; what it reads is listed on its own.
      if (!known.isField) continue;
      if (
         (known.unresolved.length > 0 && known.declared.length === 0) ||
         (known.ceilingUnresolved && declaresCarries)
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
      // An output of a query reads from the query's source, not from its own siblings.
      const reads: OutputReads = (out
         ? out.outputs.reads.get(out.rel)
         : undefined) ?? {
         value: out ? [] : fieldReferences(field.e),
         filter: [],
         // An output whose reads are unknown may be a copy.
         passthrough: out ? true : isPassthrough(field.e),
      };
      const inputs: ClassifiedInput[] = [];
      const seenInput = new Set<string>();
      const unresolved: string[] = [];
      const unresolvedRead = (refPath: string): void => {
         if (!unresolved.includes(refPath)) unresolved.push(refPath);
      };
      const unread = out !== undefined && !out.outputs.reads.has(out.rel);
      if (unread) unresolvedRead(path);
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
            const refPath = owner && !out ? `${owner}.${name}` : name;
            if (refPath === path && !out) continue;
            const input = out
               ? out.outputs.base?.get(refPath)
               : resolve(refPath);
            if (!input) {
               unresolvedRead(refPath);
               continue;
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
         }
      };
      read(reads.value);
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
         isField: isAtomicField(field),
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

/** One published source and what the lineage found on it. */
export interface PublishedSource {
   modelPath: string;
   source: string;
   ir: IrField;
   /** The strictest `#(carries)` the source is held to: its own and its query's base's. */
   carries: { declared: boolean; classes: Set<string>; malformed?: string };
   /** Whether the compiled model declares a `#(carries)` anywhere, which makes an unreadable struct a refusal. */
   modelCarries: boolean;
   deniedToAll: boolean;
   /** Reachable dimensions and measures a caller can read, by path. */
   fields: Array<[string, FieldClassification]>;
   /** Hidden dimensions and measures a view or a reachable field reads, directly or through other fields, by path. */
   hiddenRead: Array<[string, FieldClassification]>;
   /** What each of its views reads from it. */
   views: ViewExposure[];
   /** True when its join walk hit a bound, so fields past it were not read. */
   truncated: boolean;
}

/**
 * True when this source's declaration was already visited through another
 * model, and records it otherwise. A source with no recorded location is
 * keyed to its own model, so it is never skipped.
 */
export function alreadySeen(
   seen: Set<string>,
   model: Model,
   modelPath: string,
   name: string,
): boolean {
   const key = model.definitionIdentity(name) ?? `${modelPath}\0${name}`;
   if (seen.has(key)) return true;
   seen.add(key);
   return false;
}

/**
 * Every source the package publishes, with its reachable fields classified.
 * `isListed` says which models are on the surface, the same answer
 * `listModels()` gives; `getSourceInfos()` says which sources a listed model
 * exports. A model with no compiled definition contributes nothing.
 * `includeHidden` also takes the sources a model does not export, each once
 * however many models import it.
 */
export function publishedSources(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
   includeHidden = false,
): PublishedSource[] {
   const out: PublishedSource[] = [];
   const seen = new Set<string>();
   for (const [modelPath, model] of models) {
      if (!isListed(modelPath)) continue;
      let sourceInfos: ReturnType<Model["getSourceInfos"]>;
      let apiSources: ReturnType<Model["getSources"]>;
      let modelDef: ModelDef | undefined;
      try {
         sourceInfos = model.getSourceInfos(includeHidden);
         apiSources = model.getSources(includeHidden);
         modelDef = model.getModelDef();
      } catch {
         continue;
      }
      for (const sourceInfo of sourceInfos ?? []) {
         if (
            includeHidden &&
            alreadySeen(seen, model, modelPath, sourceInfo.name)
         ) {
            continue;
         }
         const ir = findIrSource(modelDef, sourceInfo.name);
         if (!ir) continue;
         const apiSource = apiSources?.find((s) => s.name === sourceInfo.name);
         const lineage = readSourceClassifications(ir.fields, ir.query);
         const views = readViewExposures(ir.fields, lineage);
         const fields = Array.from(lineage).filter(
            ([, c]) => c.isField && !c.hidden,
         );
         const hiddenRead: Array<[string, FieldClassification]> = [];
         const visited = new Set<string>();
         const visit = (path: string): void => {
            if (visited.has(path)) return;
            visited.add(path);
            const c = lineage.get(path);
            if (!c) return;
            if (c.hidden && c.isField) hiddenRead.push([path, c]);
            for (const input of c.inputs) visit(input.field);
         };
         for (const v of views) for (const r of v.reads) visit(r.field);
         for (const [, c] of fields) for (const i of c.inputs) visit(i.field);
         const chain = structCeilings(modelDef, ir, sourceInfo.name);
         out.push({
            modelPath,
            source: sourceInfo.name,
            ir,
            carries: strictestCarries(
               chain.ceilings,
               chain.unresolved,
               modelDef,
            ),
            modelCarries: modelDeclaresCarries(modelDef),
            deniedToAll: apiSource ? isDeniedToAll(apiSource) : false,
            fields,
            hiddenRead,
            views,
            truncated: lineage.truncated,
         });
      }
   }
   return out;
}

/** The unresolved reads of a field that declares its own class, which the declaration cannot vouch for. */
function declaredUnknownReads(c: FieldClassification): string[] {
   return c.declared.length > 0 ? c.unresolved : [];
}

/** The refusal for a plain copy declared below, or over, what it copies, or for an output whose reads are unknown; undefined for any other field. */
function copyLeak(
   where: string,
   allowed: string,
   path: string,
   c: FieldClassification,
): string | undefined {
   // Its declaration cannot be the fix when what it reads is unknown.
   if (c.unread) {
      return (
         `${where} carries [${allowed}] but exposes ${path}, an output ` +
         `Publisher cannot follow to what it reads, so its class is ` +
         `unknown: a nest with stages of its own is one. Flatten the nest ` +
         `into one stage, or move it out of the source.`
      );
   }
   if (!c.passthrough) return undefined;
   const unknown = declaredUnknownReads(c);
   if (c.dropped.length === 0 && unknown.length === 0) return undefined;
   const copied = [
      ...(c.dropped.length > 0
         ? [
              c.inputs
                 .filter((i) =>
                    i.classifications.some((k) => c.dropped.includes(k)),
                 )
                 .map((i) => `${i.field} (${i.classifications.join(" ")})`)
                 .join(", "),
           ]
         : []),
      ...(unknown.length > 0
         ? [`${unknown.join(", ")}, which cannot be resolved`]
         : []),
   ].join(" and ");
   return (
      `${where} carries [${allowed}] but exposes ${path}, declared ` +
      `${c.declared.join(" ")} over a plain copy of ${copied}. A ` +
      `copy does not de-identify anything: remove the declaration ` +
      `so it keeps its input's class, or derive a new field with a ` +
      `function that does.`
   );
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
   // One finding per source, however many of its fields are malformed: the
   // warnings ride every package response and `/status` entry, so their
   // count must not grow with a package's field count.
   for (const s of sources) {
      const malformed = s.fields
         .filter(([, c]) => c.malformed)
         .map(
            ([path, c]) =>
               `Field ${path} of source ${s.source} ${c.malformed}.`,
         );
      if (malformed.length === 0) continue;
      const more = malformed.length - MAX_NAMED_FIELDS;
      warnings.push({
         model: s.modelPath,
         subject: s.source,
         code: CLASSIFICATION_MALFORMED,
         severity: "warn",
         message:
            malformed.length === 1
               ? malformed[0]
               : `Source ${s.source} has ${malformed.length} malformed ` +
                 `#(classification) notes. ` +
                 malformed.slice(0, MAX_NAMED_FIELDS).join(" ") +
                 (more > 0 ? ` (+${more} more)` : ""),
      });
   }
   for (const s of sources) {
      const carries = s.carries;
      for (const [path, c] of [...s.fields, ...s.hiddenRead]) {
         const unknown = declaredUnknownReads(c);
         if (c.dropped.length === 0 && unknown.length === 0) continue;
         // A plain copy under #(carries) is refused at load, not warned.
         if (carries.declared && c.passthrough) continue;
         const reads = [
            ...(c.dropped.length > 0
               ? [`reads fields classified ${c.dropped.join(" ")}`]
               : []),
            ...(unknown.length > 0
               ? [
                    `reads fields whose class cannot be resolved: ${unknown.join(", ")}`,
                 ]
               : []),
         ].join(" and ");
         warnings.push({
            model: s.modelPath,
            subject: s.source,
            code: CLASSIFICATION_DOWNGRADE,
            severity: "warn",
            message:
               `Field ${path} of source ${s.source} declares ` +
               `${c.declared.join(" ")} but ${reads}. Publisher cannot tell ` +
               `whether this derivation de-identifies them, so a review has ` +
               `to: check that what it returns is no longer ` +
               `${c.dropped.length > 0 ? c.dropped.join(" or ") : "what those reads carry"}.`,
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
 * Rejections for every author-declared query (a named query, a notebook cell)
 * that reads past the `#(carries)` of the source it enters, so it refuses at
 * load rather than on its first run. A caller's own queries are checked per
 * request ({@link ceilingRefusal}).
 */
export async function carriesCeilingLeaks(
   models: ReadonlyMap<string, Model>,
   isListed: (modelPath: string) => boolean,
): Promise<string[]> {
   const messages: string[] = [];
   for (const [modelPath, model] of models) {
      const modelDef = model.getModelDef();
      // A notebook is served whether or not the package lists it, so its cells are always read.
      const queries: Array<{
         label: string;
         query: unknown;
         modelDef: ModelDef | undefined;
      }> = Object.entries(isListed(modelPath) ? (modelDef?.contents ?? {}) : {})
         .filter(([, def]) => (def as { type?: string }).type === "query")
         .map(([name, query]) => ({
            label: `query ${name}`,
            query,
            modelDef,
         }));
      (await model.preparedCellQueries()).forEach((cell, i) =>
         queries.push({ label: `notebook cell ${i + 1}`, ...cell }),
      );
      for (const { label, query, modelDef: def } of queries) {
         const refusal = ceilingRefusal(def, query);
         if (refusal) messages.push(`${modelPath}: ${label}: ${refusal}.`);
      }
   }
   return messages;
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
   includeHidden = false,
): string[] {
   const messages: string[] = [];
   for (const s of publishedSources(models, isListed, includeHidden)) {
      const carries = s.carries;
      const where = `source ${s.source} (${s.modelPath})`;
      // A read through a carried join is held to that join's list even when the source carries none.
      for (const [path, c] of s.fields) {
         if (c.ceilingUnresolved && s.modelCarries) {
            messages.push(
               `${where} exposes ${path}, which reads through a source whose #(carries) Publisher cannot read, so what holds it is unknown.`,
            );
         }
         // The entry's own #(carries), when it declares one, refuses the copy below.
         const copy =
            c.ceilings.length > 0 && !carries.declared
               ? copyLeak(
                    `${where} joins ${c.ceilings[0].source}, which`,
                    c.ceilings[0].classes.join(", "),
                    path,
                    c,
                 )
               : undefined;
         if (copy) messages.push(copy);
         for (const ceiling of c.ceilings) {
            const off = c.classifications.filter(
               (k) => !ceiling.classes.includes(k),
            );
            if (c.classifications.length > 0 && off.length === 0) continue;
            messages.push(
               `${where} exposes ${path}, ${
                  off.length > 0
                     ? `classified ${off.join(" ")}`
                     : "which has no class"
               }, through ${ceiling.source}, which carries [${ceiling.classes.join(", ")}].`,
            );
         }
      }
      if (!carries.declared) continue;
      if (carries.malformed) {
         messages.push(`${where} ${carries.malformed}.`);
         continue;
      }
      const allowed = Array.from(carries.classes).sort(byCodepoint).join(", ");
      if (s.truncated) {
         messages.push(
            `${where} carries [${allowed}] but joins more than ` +
               `${MAX_JOIN_RECURSION_DEPTH} deep or past ${MAX_JOIN_PATHS} ` +
               `join paths, beyond which Publisher cannot read what its ` +
               `fields carry. Drop the joins past that, or split the source.`,
         );
      }
      for (const view of s.views) {
         const off = view.reads.filter((r) =>
            r.classifications.some((k) => !carries.classes.has(k)),
         );
         if (off.length > 0) {
            const reads = off
               .map(
                  (r) =>
                     `${r.field}, classified ${r.classifications.join(" ")}`,
               )
               .join("; ");
            messages.push(
               `${where} carries [${allowed}] but its view ${view.name} reads ` +
                  `${reads}. A view returns what it groups by, aggregates and ` +
                  `filters on. Remove the view from the source, or move the ` +
                  `filter into a declared measure of a class the source carries.`,
            );
         }
         if (view.unread) {
            messages.push(
               `${where} carries [${allowed}] but its view ${view.name} has no ` +
                  `reference summary, so what it reads cannot be known. ` +
                  `Remove the view from the source.`,
            );
         }
         if (view.unclassified.length > 0) {
            messages.push(
               `${where} carries [${allowed}] but its view ${view.name} reads ` +
                  `${view.unclassified.join(", ")}, which has no class or ` +
                  `cannot be resolved against the source (a field the view ` +
                  `declares with extend: is one). Declare the field on the ` +
                  `source with a #(classification) and read it from there.`,
            );
         }
      }
      // An undeclared field already carries its filter's classes in its own.
      const filterLeak = (c: FieldClassification): void => {
         const filtered = c.filtered.filter((i) =>
            i.classifications.some((k) => !carries.classes.has(k)),
         );
         if (c.declared.length > 0 && filtered.length > 0) {
            const filters =
               `${where} carries [${allowed}] but its query filters on ` +
               `${filtered.map((i) => `${i.field} (${i.classifications.join(" ")})`).join(", ")}, ` +
               `which every row it returns reveals. Remove the filter, or ` +
               `add the class to #(carries) if the policy allows it.`;
            if (!messages.includes(filters)) messages.push(filters);
         }
      };
      for (const [path, c] of s.hiddenRead) {
         const copy = copyLeak(where, allowed, path, c);
         if (copy) messages.push(copy);
         else filterLeak(c);
      }
      for (const [path, c] of s.fields) {
         const copy = copyLeak(where, allowed, path, c);
         if (copy) {
            messages.push(copy);
            continue;
         }
         filterLeak(c);
         const off = c.classifications.filter((k) => !carries.classes.has(k));
         if (c.classifications.length > 0 && off.length === 0) {
            if (c.unresolved.length > 0 && c.declared.length === 0) {
               messages.push(
                  `${where} carries [${allowed}] but exposes ${path}, which ` +
                     `reads ${c.unresolved.join(", ")}, which cannot be ` +
                     `resolved against the source, so its class is unknown. ` +
                     `Declare a #(classification) on ${path}.`,
               );
            }
            continue;
         }
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
