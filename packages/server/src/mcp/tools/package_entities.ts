// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import path from "path";
import { fileURLToPath } from "url";
import lunr from "lunr";
import type { Relationship } from "@malloydata/malloy-interfaces";
import type { ModelDef } from "@malloydata/malloy";
import { EnvironmentStore } from "../../service/environment_store";
import { Package } from "../../service/package";
import { referencedGivenNames } from "../../service/authorize";
import { logger } from "../../logger";
import { entityRowKey, KEY_SEPARATOR } from "./embedding_index";
import type { PipelineSettings } from "./get_context_pipeline";
import { compareText } from "./retrieval_shared";
import type {
   Entity,
   JoinReach,
   JoinTopology,
   PackageIndex,
   SourceContextAuthorize,
   SourceContextEntry,
   SourceContextFilter,
   SourceContextGiven,
} from "./get_context_types";

/**
 * Caps on doc text carried as CONTEXT rather than as a result. A source's own
 * doc arrives in full when the source is itself a hit; these caps apply only
 * to the copy that rides along with a field hit, where the point is to deliver
 * the grain caveat, not to reproduce the model file.
 */
export const SOURCE_DOC_MAX_CHARS = 500;
export const JOIN_DOC_MAX_CHARS = 200;

function truncateDoc(doc: string, max: number): string {
   if (doc.length <= max) return doc;
   const cut = doc.slice(0, max);
   const lastSpace = cut.lastIndexOf(" ");
   return `${(lastSpace > max * 0.6 ? cut.slice(0, lastSpace) : cut).trimEnd()}…`;
}

/**
 * Pull #(doc) text from annotation lines, falling back to the raw lines.
 * SourceInfo sources/fields carry Annotation objects ({ value }); named queries
 * carry raw strings, so accept both.
 */
/**
 * Extract ONLY `#(doc)` annotation text, empty when there is none. This is
 * the safe input for embedding: unlike docText it never falls back to the
 * raw annotation lines, so predicate-bearing annotations (`#(access_filter)`
 * row-level-security rules, tenant lists, `#(malloy)` internals) are never
 * sent to an external embedding provider.
 */
export function docOnlyText(
   annotations?: Array<string | { value: string }>,
): string {
   if (!annotations || annotations.length === 0) return "";
   const docs = annotations
      .map((a) => (typeof a === "string" ? a : a.value))
      .map((a) => a.match(/#\(doc\)\s*(.*)/)?.[1]?.trim() ?? "")
      .filter(Boolean);
   return docs.join(" ").replace(/\s+/g, " ").trim();
}

/**
 * Human-facing doc text for the response `doc` field. Prefers `#(doc)`
 * text and falls back to the raw annotation lines when there is none.
 * Pre-existing lexical behaviour; NOT used as embedding input (see
 * docOnlyText and Entity.embedDoc).
 */
export function docText(
   annotations?: Array<string | { value: string }>,
): string {
   const doc = docOnlyText(annotations);
   if (doc) return doc;
   if (!annotations || annotations.length === 0) return "";
   const lines = annotations.map((a) => (typeof a === "string" ? a : a.value));
   return lines.join(" ").replace(/\s+/g, " ").trim();
}

/**
 * How many joins deep a field path is indexed. Depth 1 reaches
 * `orders.amount`; depth 2 reaches `order_items.inventory_items.cost`, the
 * depth the published shape's own `join_path` example uses. Deeper paths
 * exist and stay unindexed: each level multiplies the entity count by the
 * joined source's field count, against the entity cap
 * (`retrieval.indexing.maxEntities`).
 */
const MAX_JOIN_PATH_DEPTH = 2;

/**
 * What the compiled model knows about a field that the stable SourceInfo does
 * not: the expression it was defined by. `code` is the authored text
 * ("sale_price.sum()"); `aliasOf` is set only when the whole definition is a
 * reference to a sibling field of the same source.
 */
interface FieldProvenance {
   code?: string;
   aliasOf?: string;
}

/**
 * Read per-field provenance for one source out of the compiled model IR.
 *
 * Keyed by field name so the caller can join it onto the curated
 * `SourceInfo.schema.fields` it already walks. The IR is deliberately
 * uncurated (see Model.getModelDef), so it is used as a LOOKUP for fields the
 * curated list already admitted, never as a field list of its own.
 *
 * The alias rule is `e.node === "field"` with a SINGLE-segment path, and that
 * restriction carries the whole correctness argument. It is not theoretical:
 * in the bundled storefront model `order_items.category`, `.brand` and
 * `.region` are all bare field references -- to `["products","category"]`,
 * `["products","brand"]` and `["regions","region"]`. Those traverse a join to
 * reach a DIFFERENT column, and folding them into their target is exactly the
 * error this guard exists to prevent. Only a single-segment path names a
 * sibling field of the same source.
 */
/**
 * The ACTIVE name of a compiled entry: `as` when a rename set one, else
 * `name`. This is what `to_stable` puts in SourceInfo, so it is what a caller
 * joining IR onto SourceInfo has to match on. Keying the raw `name` gave every
 * renamed field no provenance at all -- `include { rename: b is a }` compiles
 * to `{name: "a", as: "b"}`, so the map said "a" while the lookup asked "b".
 */
function activeName(v: { name?: string; as?: string }): string | undefined {
   return v.as ?? v.name;
}

/**
 * The text a `DocumentLocation` range covers, or undefined if the range does
 * not fit the file. Ranges are zero-based, end-exclusive on the character.
 */
export function sliceRange(
   text: string,
   range: {
      start: { line: number; character: number };
      end: { line: number; character: number };
   },
): string | undefined {
   const lines = text.split("\n");
   const { start, end } = range;
   if (start.line < 0 || end.line >= lines.length || end.line < start.line) {
      return undefined;
   }
   if (start.line === end.line) {
      return lines[start.line].slice(start.character, end.character);
   }
   return [
      lines[start.line].slice(start.character),
      ...lines.slice(start.line + 1, end.line),
      lines[end.line].slice(0, end.character),
   ].join("\n");
}

/**
 * The text each compiled model was built FROM, looked up by the file URL its
 * IR reports, and remembered for the life of one index build.
 *
 * A view's definition is not in the IR (see readFieldProvenance), so it is
 * sliced out of the model file at the range the IR gives. Those two have to
 * come from one revision of the file, which is why this reads the snapshot the
 * compile kept (`Model.getCompiledSourceText`) and deliberately does NOT fall
 * back to reading the file now.
 *
 * Reading it now is wrong whenever disk is ahead of the compiled model, and
 * that is a state Publisher serves on purpose: a package whose most recent
 * reload failed to compile keeps answering from the model compiled before that
 * save (`get_status` reports it as `stale: true`), and the index is cached per
 * Package object, so a failed reload leaves the cache intact and a first
 * `get_context` inside that window would cut post-edit bytes at pre-edit
 * coordinates. The result is not a missing field, it is confident text that is
 * not the view -- mid-token, or another field's definition -- beside an
 * `execute_query` that still runs the old one. An edit that is never reloaded
 * at all produces the same disagreement without ever being marked stale.
 *
 * A model with no snapshot (a `.malloynb`, a compile failure) yields no view
 * code. Nothing else on the card changes.
 */
function makeSourceTextReader(
   pkg: Package,
): (url: string) => string | undefined {
   const cache = new Map<string, string | undefined>();
   // Optional-chained like the model accessors in collectEntities: a spec's
   // package stand-in implements only what it needs.
   const packagePath = pkg.getPackagePath?.();
   return (url: string) => {
      if (cache.has(url)) return cache.get(url);
      let text: string | undefined;
      try {
         if (url.startsWith("file:") && packagePath) {
            // The IR's URL was built as packagePath + modelPath by the loader,
            // so the same join inverts it. A path outside the package is not a
            // model this package compiled and has no snapshot.
            const relative = path
               .relative(packagePath, fileURLToPath(url))
               .split(path.sep)
               .join("/");
            text =
               relative && !relative.startsWith("..")
                  ? pkg.getModel?.(relative)?.getCompiledSourceText?.()
                  : undefined;
         }
      } catch {
         text = undefined;
      }
      cache.set(url, text);
      return text;
   };
}

/** One source's compiled definition, found by the name SourceInfo reports. */
function findSourceDef(
   modelDef: ModelDef | undefined,
   sourceName: string,
): { fields?: unknown[]; annotations?: unknown } | undefined {
   const contents = modelDef?.contents;
   if (!contents) return undefined;
   return Object.values(contents).find(
      (candidate) =>
         activeName(candidate as { name?: string; as?: string }) === sourceName,
   ) as { fields?: unknown[]; annotations?: unknown } | undefined;
}

/**
 * A source's OWN documentation, excluding anything it inherited by extending
 * another source. Undefined when the compiled model is unavailable, so the
 * caller can fall back rather than silently reporting no doc.
 *
 * `extend` exists to CHANGE a source -- a filter, a join, a narrowed grain --
 * so the parent's description is not a description of the child. Malloy keeps
 * the two apart in the IR:
 *
 *   annotations: {
 *     blockNotes: [ ...the source's own #(doc) lines ],
 *     inherits: { blockNotes: [ ...the parent's ] },
 *   }
 *
 * `to_stable` flattens that to one array with the INHERITED notes first, and
 * reading the flattened form is what made `scoped_orders` -- an `order_items
 * extend { where: ... }` -- report order_items' description, and made its
 * one_line_summary (a first sentence) identical to its parent's. A source that
 * extends without documenting itself now reports no doc, which is true and
 * fixable, rather than a confident description of a different source.
 *
 * Field-level docs are deliberately untouched: an inherited dimension or
 * measure IS the parent's field, so the doc it carries is its own and correct.
 */
function readSourceOwnDoc(
   modelDef: ModelDef | undefined,
   sourceName: string,
): string[] | undefined {
   const annotations = findSourceDef(modelDef, sourceName)?.annotations as
      | { blockNotes?: Array<{ text?: string }> }
      | undefined;
   if (!annotations) return undefined;
   return (annotations.blockNotes ?? [])
      .map((note) => note.text ?? "")
      .filter(Boolean);
}

function readFieldProvenance(
   modelDef: ModelDef | undefined,
   sourceName: string,
   sourceTextFor?: (url: string) => string | undefined,
): Map<string, FieldProvenance> {
   const provenance = new Map<string, FieldProvenance>();
   const entry = findSourceDef(modelDef, sourceName);
   if (!entry?.fields) return provenance;
   for (const raw of entry.fields) {
      const field = raw as {
         name?: string;
         as?: string;
         code?: string;
         type?: string;
         location?: {
            url?: string;
            range?: {
               start: { line: number; character: number };
               end: { line: number; character: number };
            };
         };
         e?: { node?: string; path?: string[] };
      };
      const fieldName = activeName(field);
      if (!fieldName) continue;
      const referent =
         field.e?.node === "field" && field.e.path?.length === 1
            ? field.e.path[0]
            : undefined;
      // Malloy fills `code` for a scalar expression only, so a view -- whose
      // definition is a query pipeline -- arrives with none, exactly like a
      // physical column. Its `location` does cover the definition though, so
      // the text comes off the file instead. Restricted to turtles because a
      // physical column's location points at the source's table() expression,
      // which would slice to something that is not the field at all.
      const viewCode =
         field.type === "turtle" && field.location?.url && field.location.range
            ? sliceRange(
                 sourceTextFor?.(field.location.url) ?? "",
                 field.location.range,
              )
            : undefined;
      provenance.set(fieldName, {
         ...(field.code
            ? { code: field.code }
            : viewCode
              ? { code: viewCode }
              : {}),
         // A self-reference cannot occur in valid Malloy; guarded anyway,
         // because recording one would make a field its own alias and drop it.
         ...(referent && referent !== fieldName ? { aliasOf: referent } : {}),
      });
   }
   return provenance;
}

/** The shape of a field inside a join's inlined schema. */
interface JoinSchemaField {
   kind?: string;
   name: string;
   annotations?: Array<string | { value: string }>;
   relationship?: Relationship;
   schema?: { fields?: JoinSchemaField[] };
   type?: { kind?: string };
}

/**
 * Step into a nested join: the dotted path so far and the widest relationship
 * on it. `collectJoinedFields` (index-time copies) and `buildJoinReaches` (the
 * topology) both walk joins with this, so a path or fan-out is computed one
 * way. A `one` hop keeps the parent's fan-out; any other relationship
 * replaces it.
 */
export function enterJoin(
   join: { name: string; relationship?: Relationship },
   from: { joinPath: string; fanout: Relationship },
): { joinPath: string; fanout: Relationship } {
   return {
      joinPath: `${from.joinPath}.${join.name}`,
      fanout:
         join.relationship === "one"
            ? from.fanout
            : (join.relationship ?? from.fanout),
   };
}

/**
 * The deepest join chain the topology records. Assembly reads it through
 * `settings.joinMaxDepth`, which may be lower. It is separate from
 * MAX_JOIN_PATH_DEPTH because the topology holds paths, not one entity per
 * path, so a deep chain costs nothing against the embedded entity cap.
 */
export const JOIN_TOPOLOGY_MAX_DEPTH = 10;

/** The part of a compiled join entry the topology reads. */
interface CompiledJoin {
   name?: string;
   as?: string;
   join?: string;
   sourceID?: string;
   referenceID?: string;
   fields?: unknown[];
}

/**
 * The name of the source a compiled join points at, and the URL of the file
 * that defines it. The IR join entry's own `name` is the underlying table or
 * SQL, not the source; the source is in `sourceID`, written
 * "sourceName@modelURL". Undefined for a join with no such id (an inline
 * table), which has no source to reach.
 */
function joinTargetSource(
   join: CompiledJoin,
): { source: string; url: string } | undefined {
   const id = join.sourceID ?? join.referenceID;
   if (typeof id !== "string") return undefined;
   // Greedy, so a name containing "@" still splits at the URL's own "@".
   const match = /^(.+)@([a-z][a-z0-9+.-]*:\/\/.*)$/i.exec(id);
   return match ? { source: match[1], url: match[2] } : undefined;
}

/**
 * The package-relative path of the model file a URL names: the longest model
 * path the URL ends with, at a directory boundary. Undefined when the URL is
 * not one of the package's models (an import from outside the package).
 */
export function modelPathOfUrl(
   url: string,
   modelPaths: readonly string[],
): string | undefined {
   let pathname: string;
   try {
      pathname = decodeURIComponent(new URL(url).pathname);
   } catch {
      return undefined;
   }
   let best: string | undefined;
   for (const modelPath of modelPaths) {
      if (pathname !== modelPath && !pathname.endsWith(`/${modelPath}`)) {
         continue;
      }
      if (best === undefined || modelPath.length > best.length) {
         best = modelPath;
      }
   }
   return best;
}

/**
 * Every source reachable from one source by joins, up to
 * JOIN_TOPOLOGY_MAX_DEPTH deep. Aliases, relationships and nesting come from
 * the same SourceInfo join tree `collectJoinedFields` walks; the target
 * source's name is not in that tree (#1100), so it is read from the compiled
 * join entry with the same alias (`compiled`). A join with no readable target
 * records nothing, and its nested joins are still followed. A target is
 * recorded with the package-relative path of the file that defines it, so two
 * files that each define a source of the same name stay two targets; a target
 * whose file is not one of the package's models records nothing.
 */
function buildJoinReaches(args: {
   fields: JoinSchemaField[];
   compiled: unknown[] | undefined;
   from?: { joinPath: string; fanout: Relationship };
   path?: string[];
   modelPaths: readonly string[];
   out: JoinReach[];
}): void {
   const { fields, compiled, from, path = [], modelPaths, out } = args;
   for (const field of fields) {
      if (field.kind !== "join") continue;
      const entry = (compiled as CompiledJoin[] | undefined)?.find(
         (candidate) =>
            candidate.join !== undefined &&
            activeName(candidate as { name?: string; as?: string }) ===
               field.name,
      );
      const here = from
         ? enterJoin(field, from)
         : {
              joinPath: field.name,
              fanout: field.relationship as Relationship,
           };
      const hopPath = [...path, field.name];
      const target = entry ? joinTargetSource(entry) : undefined;
      const targetModelPath = target
         ? modelPathOfUrl(target.url, modelPaths)
         : undefined;
      if (target && targetModelPath !== undefined) {
         out.push({
            targetSource: target.source,
            targetModelPath,
            path: hopPath,
            fanout: here.fanout,
         });
      }
      if (hopPath.length >= JOIN_TOPOLOGY_MAX_DEPTH) continue;
      buildJoinReaches({
         fields: field.schema?.fields ?? [],
         compiled: entry?.fields,
         from: here,
         path: hopPath,
         modelPaths,
         out,
      });
   }
}

/**
 * Index the fields reachable THROUGH a join, under their dotted Malloy path.
 *
 * Indexing the join alone is not enough, even though the target source is
 * already indexed on its own: the caller needs the PATH, and the path cannot
 * be derived from anything else the response carries. The stable `JoinInfo`
 * inlines the target's schema without naming the target source (#1100), so an
 * agent holding only a join entity knows a relationship exists with no way to
 * learn what it reaches -- while the tool description tells it to "traverse a
 * join as joinName.fieldName". So each reachable field is indexed as an entity
 * in its own right, reporting its traversal as `join_path`.
 *
 * `fanout` is the widest relationship on the path, not the last one: a single
 * `many` hop anywhere means aggregating through this field fans out, which is
 * the fact that decides whether a measure reached this way can be trusted.
 * Views are deliberately not collected: a joined view is not referenceable as
 * `joinName.viewName` the way a dimension or measure is.
 */
function collectJoinedFields(args: {
   fields: JoinSchemaField[];
   sourceName: string;
   modelPath: string;
   joinPath: string;
   fanout: Relationship;
   depth: number;
   nextId: () => string;
   out: Entity[];
}): void {
   const {
      fields,
      sourceName,
      modelPath,
      joinPath,
      fanout,
      depth,
      nextId,
      out,
   } = args;
   for (const field of fields) {
      if (field.kind === "join") {
         if (depth >= MAX_JOIN_PATH_DEPTH) continue;
         collectJoinedFields({
            ...args,
            fields: field.schema?.fields ?? [],
            ...enterJoin(field, { joinPath, fanout }),
            depth: depth + 1,
         });
         continue;
      }
      if (field.kind !== "dimension" && field.kind !== "measure") continue;
      out.push({
         id: nextId(),
         kind: field.kind,
         name: `${joinPath}.${field.name}`,
         // The source that DECLARES the traversal, so the field nests under
         // the card a caller would actually query from.
         source: sourceName,
         modelPath,
         doc: docText(field.annotations),
         embedDoc: docOnlyText(field.annotations),
         joinPath,
         relationship: fanout,
         ...(field.kind === "measure" || field.kind === "dimension"
            ? { dataType: malloyType(field) }
            : {}),
      });
   }
}

/**
 * Whether `apiSource` is denied unconditionally — on EITHER route, since the
 * two routes AND together and one bare-`false` conjunct denies every caller
 * regardless of the other route or any given supplied. Only `#(authorize)`
 * accepts a `false` an author wrote; the filter route can still carry one as
 * the fail-closed placeholder for an unattributable gate, which denies just
 * as absolutely, so this keys on the deny rather than the route. Case- and
 * whitespace-insensitive: the grammar parser lowercases `FALSE` only for its
 * own comparison, so the wire payload can still carry it uppercase
 * (`authorize_grammar.ts`).
 */
function isUnconditionalDenyAuthorize(apiSource: {
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
 * Walk every model in the package and collect sources, their views,
 * dimension/measure fields and declared joins, and named queries. Returns the
 * full set; the optional source-level drill-down is applied by the caller
 * after retrieval.
 */
async function collectEntities(pkg: Package): Promise<CollectedModel> {
   // listModels() already returns only .malloy model files (notebooks are listed separately).
   // Sorted by path so the walk is deterministic. Which paths a source reports
   // no longer depends on it — every resolving path is its own card, see the
   // dedupe below — but the order sources are first seen in still decides the
   // order equally-ranked cards come back in, and `first wins` still settles
   // the odd genuine within-a-file duplicate. Filesystem order would make both
   // differ between two machines serving the same package.
   // Codepoint order, not localeCompare: with no locale argument that
   // follows the runtime's collation, which moves with LANG/LC_ALL -- so two
   // servers on identical bytes could walk the same package in different
   // orders, which is the one thing this sort exists to prevent.
   const models = [...(await pkg.listModels())].sort((a, b) =>
      compareText(a.path ?? "", b.path ?? ""),
   );

   const modelPaths = models.flatMap((m) => (m.path ? [m.path] : []));
   const entities: Entity[] = [];
   const governance = new Map<string, SourceGovernance>();
   // Names of every source dropped for an unconditional deny-all gate, package-
   // wide. Computed once per model, before either the source or the query loop
   // runs, so both can skip the same names and neither can resurrect a card for
   // one the other dropped (see the query loop and cardFor below).
   const droppedSources = new Set<string>();
   // Which sources each source reaches by joins; see buildJoinReaches.
   const topology = new Map<string, JoinReach[]>();
   // One reader for the whole walk: several sources share a model file.
   const sourceTextFor = makeSourceTextReader(pkg);
   let n = 0;
   for (const apiModel of models) {
      // path is optional in the generated API types; skip models without one.
      const modelPath = apiModel.path;
      if (!modelPath) continue;
      const model = pkg.getModel(modelPath);
      if (!model) continue;
      // SourceInfo carries the full schema (views plus dimension/measure fields);
      // named queries come from getQueries().
      const sourceInfos = model.getSourceInfos() ?? [];
      const queries = model.getQueries() ?? [];
      // The compiled ApiSource carries what SourceInfo does not: the givens in
      // scope and the gates in force. Keyed by name so the card can
      // pick up its own, and read defensively because a spec's model stand-in
      // implements only the two accessors above.
      const apiSources = model.getSources?.() ?? [];
      // The compiled IR, for the one thing SourceInfo cannot carry: a field's
      // expression. Optional-chained on the same grounds as getSources above
      // -- a spec's model stand-in implements only the accessors it needs --
      // and a model that failed to compile has none. Either way the fields
      // still index, just without provenance.
      const modelDef = model.getModelDef?.();

      for (const apiSource of apiSources) {
         if (apiSource.name && isUnconditionalDenyAuthorize(apiSource)) {
            droppedSources.add(sourceContextKey(modelPath, apiSource.name));
         }
      }

      for (const sourceInfo of sourceInfos) {
         const sourceName = sourceInfo.name;
         // An unconditional `false` on either route (any case/whitespace —
         // see isUnconditionalDenyAuthorize)
         // denies every caller with no given able to change that, so there is
         // nothing this card can offer an agent that queries it. Drop the
         // source entirely rather than list it and let the agent learn only
         // from the 403; every OTHER gate stays reported, because a caller's
         // givens are untrusted here and evaluating a real rule would be
         // forgeable (see execute_query_tool.ts).
         const governanceKey = sourceContextKey(modelPath, sourceName);
         if (droppedSources.has(governanceKey)) {
            continue;
         }
         const provenance = readFieldProvenance(
            modelDef,
            sourceName,
            sourceTextFor,
         );
         // Keyed per (model path, source) like the card it decorates, so a
         // card can never show one file's givens/gates beside another file's
         // model_path.
         if (!governance.has(governanceKey)) {
            const apiSource = apiSources.find((c) => c.name === sourceName);
            if (apiSource) {
               governance.set(governanceKey, {
                  givens: (apiSource.givens ?? []).flatMap((given) =>
                     given.name
                        ? [
                             {
                                name: given.name,
                                ...(given.type ? { type: given.type } : {}),
                                ...(given.annotations?.length
                                   ? { annotations: given.annotations }
                                   : {}),
                                ...(given.default !== undefined
                                   ? { default: given.default }
                                   : {}),
                             },
                          ]
                        : [],
                  ),
                  accessFilter: (apiSource.accessFilter ?? []).map(
                     (expression) => ({
                        expression,
                        given_names: referencedGivenNames(expression),
                     }),
                  ),
                  authorize: (apiSource.authorize ?? []).map((expression) => ({
                     expression,
                     given_names: referencedGivenNames(expression),
                  })),
                  filters: (apiSource.filters ?? []).flatMap((filter) =>
                     filter.name && filter.type
                        ? [
                             {
                                name: filter.name,
                                type: filter.type,
                                ...(filter.dimension
                                   ? { dimension: filter.dimension }
                                   : {}),
                                ...(filter.required ? { required: true } : {}),
                             },
                          ]
                        : [],
                  ),
               });
            }
         }
         // A source's own doc, never the one it inherited by extending
         // another source (see readSourceOwnDoc). Falls back to the flattened
         // SourceInfo annotations when there is no compiled model to read --
         // a spec stand-in, or a model that failed to compile -- which is the
         // pre-existing behaviour rather than a silent loss of the doc.
         const ownDoc = readSourceOwnDoc(modelDef, sourceName);
         const sourceAnnotations = ownDoc ?? sourceInfo.annotations;
         entities.push({
            id: String(n++),
            kind: "source",
            name: sourceName,
            source: sourceName,
            modelPath,
            doc: docText(sourceAnnotations),
            embedDoc: docOnlyText(sourceAnnotations),
         });
         if (!topology.has(governanceKey)) {
            const reaches: JoinReach[] = [];
            buildJoinReaches({
               fields: (sourceInfo.schema.fields ?? []) as JoinSchemaField[],
               compiled: findSourceDef(modelDef, sourceName)?.fields,
               modelPaths,
               out: reaches,
            });
            if (reaches.length > 0) topology.set(governanceKey, reaches);
         }
         for (const field of sourceInfo.schema.fields ?? []) {
            // Joins are indexed as entities in their own right: an agent that
            // cannot see a declared join concludes the model has none and
            // burns queries guessing one. The join's own #(doc) is a common
            // home for the rule that governs the relationship, so it has to
            // be retrievable. `calculate` (window) fields stay unindexed:
            // they are not referenceable outside the view that defines them.
            if (field.kind === "join") {
               entities.push({
                  id: String(n++),
                  kind: "join",
                  name: field.name,
                  // The source that DECLARES the join, so a drill-down on
                  // that source sees it. The stable JoinInfo inlines the
                  // target's schema without naming the target source, so
                  // there is no targetSource to report.
                  source: sourceName,
                  modelPath,
                  doc: docText(field.annotations),
                  embedDoc: docOnlyText(field.annotations),
                  relationship: field.relationship,
                  ...joinTargetOf(topology.get(governanceKey), field.name),
               });
               // The join's own entity above says a relationship exists; the
               // fields below say what it reaches and under what path. Both
               // are needed, and neither substitutes for the other.
               collectJoinedFields({
                  fields: (field.schema?.fields ?? []) as JoinSchemaField[],
                  sourceName,
                  modelPath,
                  joinPath: field.name,
                  fanout: field.relationship,
                  depth: 1,
                  nextId: () => String(n++),
                  out: entities,
               });
               continue;
            }
            if (
               field.kind !== "view" &&
               field.kind !== "dimension" &&
               field.kind !== "measure"
            ) {
               continue;
            }
            // A view carries `code` (sliced from the model file) but neither
            // an `aliasOf` -- a view is not a respelling of another field --
            // nor a scalar `dataType`.
            const fieldProvenance = provenance.get(field.name);
            const isView = field.kind === "view";
            entities.push({
               id: String(n++),
               kind: field.kind,
               name: field.name,
               source: sourceName,
               modelPath,
               doc: docText(field.annotations),
               embedDoc: docOnlyText(field.annotations),
               ...(field.kind === "view"
                  ? {}
                  : { dataType: malloyType(field) }),
               ...(fieldProvenance?.code ? { code: fieldProvenance.code } : {}),
               ...(!isView && fieldProvenance?.aliasOf
                  ? { aliasOf: fieldProvenance.aliasOf }
                  : {}),
            });
         }
      }

      for (const query of queries) {
         if (!query.name) continue;
         // A model-level query over an inline source (`query: q is orders
         // extend {...} -> ...`) has no source name. The response nests every
         // entity under a source card, so such a query has nowhere to be
         // returned; indexing it would let it match a `view` target, spend a
         // slot in the per-source window, and count in total_available while
         // never appearing in `returned`. Exclude it here, before it can be
         // counted anywhere, rather than at serialization.
         if (!query.sourceName) continue;
         // Same deny-all drop as the source loop above: a query over a locked
         // source is still a route to learn the source's name and shape exist.
         if (
            droppedSources.has(sourceContextKey(modelPath, query.sourceName))
         ) {
            continue;
         }
         entities.push({
            id: String(n++),
            kind: "query",
            name: query.name,
            source: query.sourceName,
            modelPath,
            doc: docText(query.annotations),
            embedDoc: docOnlyText(query.annotations),
         });
      }
   }

   // One model surfacing the same entity twice (a re-export, say) is a
   // duplicate. Two DIFFERENT models surfacing it is not: a source is queryable
   // at every path that resolves it, and each of those is its own card — so the
   // model path is part of the key.
   const seen = new Set<string>();
   const deduped = entities.filter((e) => {
      const key = entityCardKey(e);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
   });
   // A join to a source that is not in the index (dropped for a deny-all gate,
   // or never listed because the package does not export it) keeps its alias,
   // which is a field of the visible source, but not the target's real name:
   // that name would reach the LLM in the source summary prompt and the source
   // match prompt. Done once every model has been walked, because a join can
   // name a source from a later model file.
   const indexedSources = new Set(
      deduped
         .filter((e) => e.kind === "source")
         .map((e) => sourceContextKey(e.modelPath, e.name)),
   );
   for (const e of deduped) {
      if (
         e.kind === "join" &&
         e.joinTarget &&
         !indexedSources.has(
            sourceContextKey(e.joinTargetModelPath ?? "", e.joinTarget),
         )
      ) {
         delete e.joinTarget;
         delete e.joinTargetModelPath;
      }
   }
   // A join is only a route to a source's fields when that source is itself
   // indexed: a target that was dropped (a deny-all gate) or never listed has
   // no direct fields to copy, so the join reaches nothing.
   for (const [root, reaches] of topology) {
      const kept = reaches.filter((reach) =>
         indexedSources.has(
            sourceContextKey(reach.targetModelPath, reach.targetSource),
         ),
      );
      if (kept.length > 0) topology.set(root, kept);
      else topology.delete(root);
   }
   const collapsed = collapseAliases(deduped);
   const kept = new Set(collapsed);
   return {
      entities: collapsed,
      governance,
      droppedSources,
      topology,
      aliasedAway: new Set(
         deduped
            .filter((e) => !kept.has(e))
            .map((e) =>
               [e.modelPath, e.source ?? "", e.kind, e.name].join(
                  KEY_SEPARATOR,
               ),
            ),
      ),
   };
}

/** `{ joinTarget }` for the join `alias` declared by a source, or nothing when its target is unknown. */
function joinTargetOf(
   reaches: readonly JoinReach[] | undefined,
   alias: string,
): { joinTarget?: string; joinTargetModelPath?: string } {
   const reach = reaches?.find(
      (r) => r.path.length === 1 && r.path[0] === alias,
   );
   return reach
      ? {
           joinTarget: reach.targetSource,
           joinTargetModelPath: reach.targetModelPath,
        }
      : {};
}

/** What the compiled model says about querying a source, beyond its schema. */
interface SourceGovernance {
   givens: SourceContextGiven[];
   filters: SourceContextFilter[];
   accessFilter: SourceContextAuthorize[];
   authorize: SourceContextAuthorize[];
}

/** The entities of a package, plus the per-source governance beside them. */
interface CollectedModel {
   entities: Entity[];
   governance: Map<string, SourceGovernance>;
   /** Sources dropped for an unconditional deny-all gate; see isUnconditionalDenyAuthorize. */
   droppedSources: Set<string>;
   /** Joins each source reaches, keyed by sourceContextKey. */
   topology: Map<string, JoinReach[]>;
   /**
    * Fields the index folded into another spelling of the same column (see
    * collapseAliases), as modelPath, source, kind and name joined on
    * KEY_SEPARATOR. A joined copy of one is a copy of a spelling nobody
    * searches for.
    */
   aliasedAway: Set<string>;
}

/**
 * A field's Malloy type as a plain name: "string", "number", "date".
 *
 * The interface spells these `string_type`, `number_type` and so on, which is
 * an encoding rather than a name an agent would write, so the suffix comes off.
 * Absent when the interface reports no type, which a model stand-in does.
 */
function malloyType(field: { type?: { kind?: string } }): string | undefined {
   const kind = field.type?.kind;
   return kind ? kind.replace(/_type$/, "") : undefined;
}

/**
 * Collapse a field that is DEFINED AS another field of the same source into
 * it, keeping the documented spelling and reporting the rest in `aliases`.
 *
 * A model that renames a physical column without hiding the original leaves
 * both in the schema: `dimension: site is SITE` puts `site` beside `SITE` --
 * one column indexed twice, both competing for the same scarce slots.
 *
 * Detection is EXACT, read off the compiled model: `aliasOf` is set only when
 * a field's entire definition is a reference to a single-segment sibling name
 * (see readFieldProvenance). This replaces a heuristic that matched humanized
 * names, which could not tell `site is SITE` from a derivation that happened
 * to humanize the same way. The comment here used to justify that heuristic by
 * saying the expression was unavailable. It is available: the stable
 * `Malloy.DimensionInfo` does not carry it, but the compiled `ModelDef` does,
 * and Publisher holds one (see Model.getModelDef).
 *
 * Because a reference is proof rather than a guess, the differing-doc escape
 * hatch is gone. The old rule kept two spellings apart when each carried its
 * own `#(doc)`, since a doc was the only evidence they might be two concepts.
 * A proven reference settles it: they ARE one column, and two docs on one
 * column is a modelling mistake, not a second concept. The documented
 * spelling still wins, so the doc a modeller wrote is the one returned.
 *
 * The better fix remains in the model: `include { internal: ... }` hides the
 * raw column outright and the indexer honours it already, because the compiler
 * drops non-public fields before this code sees them (see the access-modifier
 * spec). This is what the tool can do for models that have not done that.
 */
function collapseAliases(entities: Entity[]): Entity[] {
   // Every same-source field a reference could name. Dimensions and measures
   // only: a source is its own namespace, a join names a relationship rather
   // than a column, and a view is a pipeline. A joined field is indexed under
   // its dotted path and belongs to the source it is reached FROM, so it can
   // never be a same-source sibling -- and its own expression may reference a
   // leaf name that collides with one of that source's own fields, which is
   // exactly the fold that must not happen.
   const foldable = (e: Entity) =>
      (e.kind === "dimension" || e.kind === "measure") && !e.joinPath;
   // Scoped to the model path as well as the source: one source resolvable
   // from several files is several cards, each with its own copy of the same
   // fields, and a fold is a statement about ONE card. Keyed on the name
   // alone, the last file walked became every card's representative and a
   // fold computed against it dropped the folded name from all of them.
   const key = (e: Entity) =>
      `${e.modelPath}\x00${e.source ?? ""}\x00${e.name}`;
   const byName = new Map<string, Entity>();
   for (const e of entities) if (foldable(e)) byName.set(key(e), e);

   /** The field this one is defined as, when the index holds it. */
   const referentOf = (e: Entity): Entity | undefined => {
      if (!e.aliasOf || !foldable(e)) return undefined;
      const target = byName.get(
         `${e.modelPath}\x00${e.source ?? ""}\x00${e.aliasOf}`,
      );
      // The referent has to be a field this index actually holds, and it may
      // not be: `include { internal: SITE }` hides the raw column from the
      // public schema while `site` still references it. That is the model
      // doing the right thing -- `site` is then the only spelling and there
      // is nothing to fold. A rename moves a field's active name without
      // rewriting its siblings' expressions, which lands here the same way.
      if (!target || target === e) return undefined;
      // A measure defined over a dimension is not another spelling of it.
      return target.kind === e.kind ? target : undefined;
   };

   // Walk each chain to its root, so `a is b` and `b is c` land in ONE group
   // rather than making `b` both a survivor and a dropped row -- which would
   // leave `a` reporting an alias of a field no longer in the index.
   const rootOf = new Map<Entity, Entity>();
   const groups = new Map<Entity, Entity[]>();
   for (const e of entities) {
      if (!foldable(e) || !e.aliasOf) continue;
      const seen = new Set<Entity>([e]);
      let node = e;
      for (;;) {
         const next = referentOf(node);
         // A cycle cannot occur in valid Malloy; the guard costs nothing and
         // turns a malformed model into "no fold" instead of a hang.
         if (!next || seen.has(next)) break;
         seen.add(next);
         node = next;
      }
      if (node === e) continue;
      rootOf.set(e, node);
      const group = groups.get(node);
      if (group) group.push(e);
      else groups.set(node, [e]);
   }

   // Model-path scoped for the same reason `key` is: the row being removed is
   // one card's row, not the name everywhere it appears.
   const droppedKey = (e: Entity) => entityCardKey(e);
   const dropped = new Map<string, Entity>();
   for (const [root, refs] of groups) {
      const members = [root, ...refs];
      // The NAME that survives is an authored alias, never the raw column it
      // points at: `dimension: site is SITE` says the modeller wants `site`
      // used, and it is the spelling that still works once they hide the raw
      // one with `include { internal: SITE }`. Direction is the thing the old
      // name-matching rule could not know, so it had to guess from casing.
      // Among several authored names (a chain, or two names for one column)
      // prefer a documented one, then be deterministic.
      const candidates = refs.length > 0 ? refs : members;
      const keep = [...candidates].sort((a, b) => {
         if (Boolean(b.embedDoc) !== Boolean(a.embedDoc)) {
            return b.embedDoc ? 1 : -1;
         }
         // Codepoint, for the same reason the model walk uses it: this
         // picks WHICH name survives a collapse, so a locale-dependent
         // comparison would let two servers answer with different spellings
         // of the same field.
         return compareText(a.name, b.name);
      })[0];
      // The DOC survives independently of the name. A raw column that carries
      // the only `#(doc)` in the group would otherwise lose it, and `aliases`
      // carries names, not text. Two members documented DIFFERENTLY is a
      // modelling mistake rather than two concepts -- the reference proves one
      // column -- so the survivor's own doc wins and the other is dropped.
      if (!keep.embedDoc) {
         const donor = members.find((e) => e !== keep && e.embedDoc);
         if (donor) {
            keep.doc = donor.doc;
            keep.embedDoc = donor.embedDoc;
         }
      }
      const folded = members.filter((e) => e !== keep);
      keep.aliases = folded.map((e) => e.name);
      for (const e of folded) {
         dropped.set(droppedKey(e), e);
      }
   }
   if (dropped.size === 0) return entities;
   return entities.filter((e) => !dropped.has(droppedKey(e)));
}

/**
 * Per-source state is keyed by (model path, source name), not source name
 * alone. A source is resolvable — and queryable — from its own file AND from
 * every file that imports it, so the same name legitimately carries different
 * paths, and each pairing is its own card. Keying on the name alone collapsed
 * them to whichever model was walked first.
 *
 * Joined on KEY_SEPARATOR, not `|`, for the reason its own docblock gives: a
 * Malloy identifier may be backtick-quoted and a `|` is legal inside one, so
 * joining on it is not injective — ("a|b", "c") and ("a", "b|c") produce one
 * key. A control character cannot appear in an identifier, nor in a path.
 */
export function sourceContextKey(modelPath: string, source: string): string {
   return [modelPath, source].join(KEY_SEPARATOR);
}

/**
 * The key of one CARD: an entity under one of the model paths it resolves in.
 * `entityRowKey` identifies the entity, and the model path is what makes two
 * cards for one entity distinct. Same separator, same injectivity argument as
 * {@link sourceContextKey}.
 */
export function entityCardKey(e: {
   modelPath: string;
   kind: string;
   source?: string;
   name: string;
}): string {
   return (
      e.modelPath + KEY_SEPARATOR + entityRowKey(e.kind, e.source ?? "", e.name)
   );
}

/** Longest a one-line summary may be, matching the hosted API's own cap. */
const ONE_LINE_SUMMARY_MAX_CHARS = 120;

/**
 * A source's doc reduced to one line: its first sentence or line, capped.
 *
 * A catalog view wants a label, not a paragraph, and the full text is still
 * on the card as `docs`. Undocumented sources get nothing rather than a
 * fabricated summary, which is what an absent field means on both sides.
 */
function oneLineSummary(doc: string): string | undefined {
   const firstLine = doc.split("\n")[0]?.trim();
   if (!firstLine) return undefined;
   const sentenceEnd = firstLine.search(/[.!?](\s|$)/);
   const summary =
      sentenceEnd === -1 ? firstLine : firstLine.slice(0, sentenceEnd + 1);
   return summary.length > ONE_LINE_SUMMARY_MAX_CHARS
      ? truncateDoc(summary, ONE_LINE_SUMMARY_MAX_CHARS)
      : summary;
}

/**
 * Derive per-source context from the collected entities: the source's own doc,
 * every join declared on it, and the givens and gates that govern querying it. Built once per package alongside the index,
 * so attaching it to a response costs a lookup rather than a model walk.
 */
function buildSourceContext(
   collected: CollectedModel,
): Map<string, SourceContextEntry> {
   const { entities, governance } = collected;
   const context = new Map<string, SourceContextEntry>();
   for (const e of entities) {
      if (e.kind !== "source") continue;
      const gates = governance.get(sourceContextKey(e.modelPath, e.name));
      const summary = oneLineSummary(e.doc);
      context.set(sourceContextKey(e.modelPath, e.name), {
         name: e.name,
         modelPath: e.modelPath,
         doc: truncateDoc(e.doc, SOURCE_DOC_MAX_CHARS),
         joins: [],
         ...(summary ? { oneLineSummary: summary } : {}),
         ...(gates?.givens.length ? { givens: gates.givens } : {}),
         ...(gates?.accessFilter.length
            ? { accessFilter: gates.accessFilter }
            : {}),
         ...(gates?.authorize.length ? { authorize: gates.authorize } : {}),
         ...(gates?.filters.length ? { filters: gates.filters } : {}),
      });
   }
   for (const e of entities) {
      if (e.kind !== "join" || !e.relationship) continue;
      // A join declared on a source the collector never emitted (defensive:
      // every join reaches us through its source) has nowhere to hang.
      const parent = e.source
         ? context.get(sourceContextKey(e.modelPath, e.source))
         : undefined;
      if (!parent) continue;
      parent.joins.push({
         name: e.name,
         relationship: e.relationship,
         ...(e.doc ? { doc: truncateDoc(e.doc, JOIN_DOC_MAX_CHARS) } : {}),
      });
   }
   return context;
}

// Cache the built entity index per Package instance. environment.getPackage()
// serves a cached Package, and a reload swaps in a new instance, so a stale entry
// is dropped automatically (WeakMap) and the next call rebuilds.
const indexCache = new WeakMap<Package, PackageIndex>();

/**
 * The entities the semantic index embeds and searches. With joins made at
 * assembly that is the direct fields only: a joined copy would be embedded
 * again under a longer name, take a window slot from the field it copies and
 * never be damped. The lexical path is not affected; it ranks
 * `retrievalEntities`.
 */
export function embeddedEntitiesOf(
   pkgIndex: PackageIndex,
   settings: PipelineSettings,
): readonly Entity[] {
   return settings.joins === "assembly"
      ? pkgIndex.directEntities
      : pkgIndex.retrievalEntities;
}

/**
 * What the semantic index embeds when joined copies are made at assembly: every
 * entity except a joined copy that assembly will rebuild from the topology.
 *
 * Assembly rebuilds a joined copy from a direct field of the join's target
 * source, so a copy is left out only when the topology reaches a target source
 * of that file and the target has a direct field of that name, or folded that
 * name into another spelling of the same column (`aliasedAway`). A joined
 * field assembly cannot rebuild stays in, so it stays searchable. Two cases:
 *  - the join has no source to copy from (an inline table or SQL), or its
 *    target is not indexed;
 *  - the join adds the field to its target (`join_one: c is cust extend {...}`),
 *    so the target has no such field of its own.
 */
function directEntitiesOf(
   retrievalEntities: readonly Entity[],
   topology: JoinTopology,
   aliasedAway: ReadonlySet<string>,
): readonly Entity[] {
   const directFields = new Set([
      ...retrievalEntities
         .filter((e) => !e.joinPath)
         .map((e) =>
            [e.modelPath, e.source ?? "", e.kind, e.name].join(KEY_SEPARATOR),
         ),
      ...aliasedAway,
   ]);
   const rebuilt = (e: Entity): boolean => {
      if (!e.joinPath) return false;
      const reach = topology
         .get(sourceContextKey(e.modelPath, e.source ?? ""))
         ?.find((r) => r.path.join(".") === e.joinPath);
      if (!reach) return false;
      const field = e.name.slice(e.joinPath.length + 1);
      return directFields.has(
         [reach.targetModelPath, reach.targetSource, e.kind, field].join(
            KEY_SEPARATOR,
         ),
      );
   };
   return Object.freeze(retrievalEntities.filter((e) => !rebuilt(e)));
}

/** Get, or lazily build and cache, the lunr entity index for a package. */
export async function getPackageIndex(
   environmentStore: EnvironmentStore,
   environmentName: string,
   packageName: string,
): Promise<PackageIndex> {
   const environment = await environmentStore.getEnvironment(
      environmentName,
      false,
   );
   const pkg = await environment.getPackage(packageName, false);
   return packageIndexOf(pkg, packageName);
}

/** The entity index for `pkg` itself, built once per Package instance. */
export async function packageIndexOf(
   pkg: Package,
   packageName: string,
): Promise<PackageIndex> {
   const cached = indexCache.get(pkg);
   if (cached) return cached;

   const collected = await collectEntities(pkg);
   const { entities } = collected;
   const byId = new Map(entities.map((e) => [e.id, e]));
   const index = lunr(function () {
      this.ref("id");
      this.field("name", { boost: 4 });
      this.field("source");
      this.field("doc");
      for (const e of entities) {
         this.add({
            id: e.id,
            name: e.name,
            source: e.source ?? "",
            doc: e.doc,
         });
      }
   });
   const retrievalEntities = Object.freeze(Array.from(byId.values()));
   const built: PackageIndex = {
      pkg,
      byId,
      retrievalEntities,
      directEntities: directEntitiesOf(
         retrievalEntities,
         collected.topology,
         collected.aliasedAway,
      ),
      topology: collected.topology,
      index,
      entityCount: entities.length,
      sourceContext: buildSourceContext(collected),
      droppedSources: collected.droppedSources,
   };
   indexCache.set(pkg, built);
   logger.debug("[MCP Tool getContext] Built and cached entity index", {
      packageName,
      entityCount: entities.length,
   });
   return built;
}
