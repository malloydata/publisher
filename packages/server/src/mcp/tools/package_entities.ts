// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import path from "path";
import { fileURLToPath } from "url";
import lunr from "lunr";
import { parseAnnotation } from "@malloydata/malloy-tag";
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
 * Caps on doc text carried as CONTEXT alongside a field hit, where the point
 * is the grain caveat, not the model file. A source that is itself a hit
 * carries its doc in full.
 */
export const SOURCE_DOC_MAX_CHARS = 500;
export const JOIN_DOC_MAX_CHARS = 200;

function truncateDoc(doc: string, max: number): string {
   if (doc.length <= max) return doc;
   const cut = doc.slice(0, max);
   const lastSpace = cut.lastIndexOf(" ");
   return `${(lastSpace > max * 0.6 ? cut.slice(0, lastSpace) : cut).trimEnd()}…`;
}

function dbmetaDescription(lines: string[]): string {
   for (const line of lines) {
      if (!/^#\(dbmeta\)\s/.test(line)) continue;
      const { tag, log } = parseAnnotation(line);
      if (log.length > 0) continue;
      const description = tag.text("description")?.trim();
      if (description) return description;
   }
   return "";
}

/**
 * Extract ONLY `#(doc)` annotation text, else the `#(dbmeta)` description,
 * empty when there is neither. The safe input for embedding: unlike docText it
 * never falls back to raw annotation lines, so `#(access_filter)` rules, tenant
 * lists and `#(malloy)` internals never reach an external embedding provider.
 */
export function docOnlyText(
   annotations?: Array<string | { value: string }>,
): string {
   if (!annotations || annotations.length === 0) return "";
   const lines = annotations.map((a) => (typeof a === "string" ? a : a.value));
   const docs = lines
      .map((a) => a.match(/#\(doc\)\s*(.*)/)?.[1]?.trim() ?? "")
      .filter(Boolean);
   const text = docs.length > 0 ? docs.join(" ") : dbmetaDescription(lines);
   return text.replace(/\s+/g, " ").trim();
}

/**
 * Human-facing doc text for the response `doc` field: `#(doc)` or `#(dbmeta)`
 * text, falling back to the raw annotation lines. Never embedding input (see
 * docOnlyText).
 * Accepts Annotation objects (sources, fields) and raw strings (queries).
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
 * How many joins deep a field path is indexed: depth 2 reaches
 * `order_items.inventory_items.cost`. Each level multiplies the entity count
 * by the joined source's field count, against the entity cap.
 */
const MAX_JOIN_PATH_DEPTH = 2;

/**
 * What the compiled model knows about a field that SourceInfo does not: `code`
 * is the authored expression ("sale_price.sum()"); `aliasOf` is set only when
 * the whole definition references a sibling field of the same source.
 */
interface FieldProvenance {
   code?: string;
   aliasOf?: string;
}

/**
 * The ACTIVE name of a compiled entry: `as` when a rename set one, else
 * `name`. This is what `to_stable` puts in SourceInfo, so it is what joining
 * IR onto SourceInfo must match on (`rename: b is a` compiles to
 * `{name: "a", as: "b"}`).
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
 * IR reports, and remembered for one index build.
 *
 * A view's definition is sliced out of the model file at the IR's range, so
 * both must come from one revision. This reads the compile's snapshot
 * (`Model.getCompiledSourceText`) and deliberately never the file on disk: a
 * package whose last reload failed (`stale: true`), or an unreloaded edit,
 * has disk ahead of the model, and slicing post-edit bytes at pre-edit
 * coordinates yields confident text that is not the view. A model with no
 * snapshot (a `.malloynb`, a compile failure) yields no view code.
 */
function makeSourceTextReader(
   pkg: Package,
): (url: string) => string | undefined {
   const cache = new Map<string, string | undefined>();
   // Optional-chained: a spec's package stand-in implements only what it needs.
   const packagePath = pkg.getPackagePath?.();
   return (url: string) => {
      if (cache.has(url)) return cache.get(url);
      let text: string | undefined;
      try {
         if (url.startsWith("file:") && packagePath) {
            // The loader built the IR's URL as packagePath + modelPath, so the
            // same join inverts it. A path outside the package has no snapshot.
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
 * A source's OWN documentation, excluding anything inherited by extending
 * another source. Undefined when the compiled model is unavailable, so the
 * caller can fall back.
 *
 * `extend` exists to CHANGE a source, so the parent's description is not the
 * child's. The IR keeps them apart (`annotations.blockNotes` vs
 * `annotations.inherits.blockNotes`), while `to_stable` flattens them with the
 * inherited notes first. A source that extends without documenting itself
 * reports no doc, which is true and fixable.
 *
 * Field docs are untouched: an inherited field IS the parent's field.
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

/**
 * Read per-field provenance for one source out of the compiled model IR, keyed
 * by active field name. The IR is uncurated (see Model.getModelDef), so it is
 * only a LOOKUP for fields the curated SourceInfo already admitted.
 *
 * An alias is `e.node === "field"` with a SINGLE-segment path. A multi-segment
 * reference (storefront's `order_items.category` -> `["products","category"]`)
 * traverses a join to a DIFFERENT column and must not be folded into it.
 */
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
      // Malloy fills `code` for scalar expressions only, so a view arrives
      // with none; its text is sliced from the file at its `location`.
      // Turtles only: a physical column's location points at the source's
      // table() expression.
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
         // A self-reference cannot occur in valid Malloy; guarded because
         // recording one would make a field its own alias and drop it.
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
 * on it, shared by collectJoinedFields and buildJoinReaches so both compute it
 * one way. A `one` hop keeps the parent's fan-out; any other replaces it.
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
 * The deepest join chain the topology records (assembly may read less via
 * `settings.joinMaxDepth`). Deeper than MAX_JOIN_PATH_DEPTH because the
 * topology holds paths, not entities, so it costs nothing against the cap.
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
 * The source a compiled join points at, and the URL of its defining file. The
 * IR join's own `name` is the table or SQL; the source is in `sourceID`,
 * "sourceName@modelURL". Undefined for an inline table.
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
 * the SourceInfo join tree; the target source's name is not in that tree, so
 * it is read from the compiled join entry with the same alias. A join with no
 * readable target records nothing but its nested joins are still followed.
 * Targets carry their package-relative model path, so same-named sources in
 * two files stay distinct; a target outside the package's models is skipped.
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
 * The target source is indexed on its own, but the caller needs the PATH,
 * which nothing else in the response carries (the stable JoinInfo does not
 * name the target source). So each reachable field is its own entity,
 * reporting its traversal as `join_path`.
 *
 * `fanout` is the widest relationship on the path: one `many` hop anywhere
 * means aggregating through the field fans out. Views are not collected: a
 * joined view is not referenceable as `joinName.viewName`.
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
         // The source that DECLARES the traversal, the card a caller queries.
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
 * Whether `apiSource` is denied unconditionally: a bare `false` conjunct on
 * EITHER route (the routes AND together) denies every caller whatever givens
 * are supplied. The filter route can carry one as the fail-closed placeholder
 * for an unattributable gate, so this keys on the deny, not the route.
 * Case-insensitive, because the wire payload can carry `FALSE` uppercase.
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
 * Walk every model in the package and collect sources, their views, fields
 * and joins, and named queries. Source drill-down is applied by the caller.
 */
async function collectEntities(pkg: Package): Promise<CollectedModel> {
   // Sorted by path, codepoint order (localeCompare follows LANG/LC_ALL), so
   // two servers on identical bytes walk the package the same way: walk order
   // decides the order of equally-ranked cards and which within-file
   // duplicate wins.
   const models = [...(await pkg.listModels())].sort((a, b) =>
      compareText(a.path ?? "", b.path ?? ""),
   );

   const modelPaths = models.flatMap((m) => (m.path ? [m.path] : []));
   const entities: Entity[] = [];
   const governance = new Map<string, SourceGovernance>();
   // Every source dropped for an unconditional deny-all gate, package-wide.
   // Filled before the source and query loops so neither can resurrect a card
   // the other dropped.
   const droppedSources = new Set<string>();
   // Which sources each source reaches by joins; see buildJoinReaches.
   const topology = new Map<string, JoinReach[]>();
   // One reader for the whole walk: several sources share a model file.
   const sourceTextFor = makeSourceTextReader(pkg);
   let n = 0;
   for (const apiModel of models) {
      const modelPath = apiModel.path;
      if (!modelPath) continue;
      const model = pkg.getModel(modelPath);
      if (!model) continue;
      const sourceInfos = model.getSourceInfos() ?? [];
      const queries = model.getQueries() ?? [];
      // The compiled ApiSource carries the givens in scope and the gates in
      // force. Read defensively: a spec's stand-in may not implement it.
      const apiSources = model.getSources?.() ?? [];
      // The compiled IR, for a field's expression. Absent for a spec stand-in
      // or a failed compile; fields then index without provenance.
      const modelDef = model.getModelDef?.();

      for (const apiSource of apiSources) {
         if (apiSource.name && isUnconditionalDenyAuthorize(apiSource)) {
            droppedSources.add(sourceContextKey(modelPath, apiSource.name));
         }
      }

      for (const sourceInfo of sourceInfos) {
         const sourceName = sourceInfo.name;
         // An unconditional `false` denies every caller, so the card offers
         // nothing: drop the source rather than let the agent learn from the
         // 403. Every OTHER gate stays reported, because a caller's givens are
         // untrusted here and evaluating a real rule would be forgeable (see
         // execute_query_tool.ts).
         const governanceKey = sourceContextKey(modelPath, sourceName);
         if (droppedSources.has(governanceKey)) {
            continue;
         }
         const provenance = readFieldProvenance(
            modelDef,
            sourceName,
            sourceTextFor,
         );
         // Keyed per (model path, source) like the card, so a card never shows
         // another file's givens or gates.
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
         // The source's own doc, not an inherited one (see readSourceOwnDoc),
         // falling back to the flattened annotations with no compiled model.
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
            // Joins are entities: an agent that cannot see a declared join
            // concludes there is none and burns queries guessing, and the
            // join's #(doc) often holds the rule for the relationship.
            // `calculate` (window) fields are not referenceable outside their
            // view, so they stay unindexed.
            if (field.kind === "join") {
               entities.push({
                  id: String(n++),
                  kind: "join",
                  name: field.name,
                  // The source that DECLARES the join, so a drill-down on it
                  // sees the join.
                  source: sourceName,
                  modelPath,
                  doc: docText(field.annotations),
                  embedDoc: docOnlyText(field.annotations),
                  relationship: field.relationship,
                  ...joinTargetOf(topology.get(governanceKey), field.name),
               });
               // The join entity says a relationship exists; the fields below
               // say what it reaches and under what path.
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
            // A view carries `code` but no `aliasOf` and no scalar `dataType`.
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
         // A model-level query over an inline source has no source name and
         // so no card to nest under; indexed, it would spend window slots and
         // count in total_available without ever being returned.
         if (!query.sourceName) continue;
         // Same deny-all drop as above: a query over a locked source would
         // reveal the source exists.
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

   // A duplicate within one model is dropped; the same entity from two
   // models is two cards (a source is queryable at every resolving path).
   const seen = new Set<string>();
   const deduped = entities.filter((e) => {
      const key = entityCardKey(e);
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
   });
   // A join whose target is not indexed (deny-all, or not exported) keeps
   // its alias but drops the target's real name, which would otherwise reach
   // the LLM prompts. After the full walk, since a join can name a source
   // from a later file.
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
   // A join reaches nothing unless its target source is itself indexed.
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
    * Fields folded into another spelling of the same column (see
    * collapseAliases), as modelPath, source, kind and name joined on
    * KEY_SEPARATOR.
    */
   aliasedAway: Set<string>;
}

/**
 * A field's Malloy type as a plain name ("string", not the interface's
 * `string_type`). Absent when the interface reports no type.
 */
function malloyType(field: { type?: { kind?: string } }): string | undefined {
   const kind = field.type?.kind;
   return kind ? kind.replace(/_type$/, "") : undefined;
}

/**
 * Collapse a field that is DEFINED AS another field of the same source into
 * it, keeping the documented spelling and reporting the rest in `aliases`.
 *
 * A model that renames a column without hiding the original
 * (`dimension: site is SITE`) indexes one column twice, both competing for
 * the same slots. Detection is EXACT: `aliasOf` is read off the compiled
 * model (see readFieldProvenance), so two spellings are proven to be one
 * column, and two docs on them is a modelling mistake, not a second concept.
 *
 * The better fix is in the model: `include { internal: ... }` hides the raw
 * column, and the compiler drops non-public fields before this code runs.
 */
function collapseAliases(entities: Entity[]): Entity[] {
   // Dimensions and measures only: a source is its own namespace, a join
   // names a relationship, and a view is a pipeline. A joined field belongs to
   // the source it is reached FROM, and its leaf name may collide with one of
   // that source's own fields, which is exactly the fold that must not happen.
   const foldable = (e: Entity) =>
      (e.kind === "dimension" || e.kind === "measure") && !e.joinPath;
   // Scoped to the model path too: a source resolvable from several files is
   // several cards, and a fold is a statement about ONE card.
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
      // The referent may not be indexed: `include { internal: SITE }` hides
      // it while `site` still references it, and then there is nothing to
      // fold. A rename lands here the same way.
      if (!target || target === e) return undefined;
      // A measure defined over a dimension is not another spelling of it.
      return target.kind === e.kind ? target : undefined;
   };

   // Walk each chain to its root, so `a is b` and `b is c` form ONE group;
   // otherwise `a` could report an alias of a field no longer in the index.
   const rootOf = new Map<Entity, Entity>();
   const groups = new Map<Entity, Entity[]>();
   for (const e of entities) {
      if (!foldable(e) || !e.aliasOf) continue;
      const seen = new Set<Entity>([e]);
      let node = e;
      for (;;) {
         const next = referentOf(node);
         // A cycle cannot occur in valid Malloy; guarded so a malformed model
         // means "no fold", not a hang.
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

   // Model-path scoped, like `key`: the removed row is one card's.
   const droppedKey = (e: Entity) => entityCardKey(e);
   const dropped = new Map<string, Entity>();
   for (const [root, refs] of groups) {
      const members = [root, ...refs];
      // The NAME that survives is an authored alias, never the raw column:
      // `site is SITE` says the modeller wants `site`, and it still works once
      // the raw column is hidden. Among several authored names prefer a
      // documented one, then be deterministic.
      const candidates = refs.length > 0 ? refs : members;
      const keep = [...candidates].sort((a, b) => {
         if (Boolean(b.embedDoc) !== Boolean(a.embedDoc)) {
            return b.embedDoc ? 1 : -1;
         }
         // Codepoint, not locale: two servers must keep the same spelling.
         return compareText(a.name, b.name);
      })[0];
      // The DOC survives independently of the name, so a raw column holding
      // the group's only `#(doc)` does not lose it. With differing docs the
      // survivor's own wins.
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
 * Per-source state is keyed by (model path, source name): a source is
 * queryable from its own file AND every file importing it, and each pairing
 * is its own card. Joined on KEY_SEPARATOR, not `|` (see its docblock).
 */
export function sourceContextKey(modelPath: string, source: string): string {
   return [modelPath, source].join(KEY_SEPARATOR);
}

/**
 * The key of one CARD: an entity under one of the model paths it resolves in.
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
 * A source's doc reduced to a catalog label: its first sentence or line,
 * capped. Undocumented sources get nothing rather than a fabricated summary.
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
 * Per-source context: the source's own doc, every join declared on it, and
 * the givens and gates that govern querying it. Built once per package, so a
 * response attaches it with a lookup.
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
      // Defensive: every join reaches us through its source.
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

// Per Package instance: a reload swaps in a new instance, so a stale entry
// drops out of the WeakMap on its own.
const indexCache = new WeakMap<Package, PackageIndex>();

/**
 * The entities the semantic index embeds and searches. With joins made at
 * assembly that is the direct fields only: a joined copy would be embedded
 * again under a longer name and take a window slot from the field it copies.
 * The lexical path ranks `retrievalEntities` regardless.
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
 * Every entity except a joined copy that assembly will rebuild from the
 * topology: one whose join reaches an indexed target source holding a direct
 * field of that name (or one folded away, `aliasedAway`). A joined field
 * assembly cannot rebuild stays searchable: an inline-table or SQL join, an
 * unindexed target, or a field the join itself adds
 * (`join_one: c is cust extend {...}`).
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
