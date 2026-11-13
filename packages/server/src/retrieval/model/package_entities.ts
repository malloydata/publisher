// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { isUnconditionalDenyAuthorize } from "../../service/entities/access_gates";
import { isOnRoute, payloadsOnRoute } from "../../service/annotation_reader";
import { hasEnvReference, warnEnvReferenceDropped } from "../../service/motly";
import path from "path";
import { fileURLToPath } from "url";
import { parseAnnotation } from "@malloydata/malloy-tag";
import type { Relationship } from "@malloydata/malloy-interfaces";
import type { ModelDef } from "@malloydata/malloy";
import type { PackageLookup } from "../../service/environment_ports";
import { Package } from "../../service/package";
import { referencedGivenNames } from "../../service/authorize";
import { splitSourceId } from "../../service/source_id";
import { identifier, MALLOY_BARE_KEYWORDS } from "../../service/malloy_text";
import {
   findIrSource,
   isClassificationLine,
   readClassification,
   readSourceClassifications,
   type FieldClassification,
} from "../../service/classification";
import { logger } from "../../logger";
import { entityRowKey, KEY_SEPARATOR } from "../index/embedding_index";
import { compareText } from "../../compare_text";
import type {
   Entity,
   JoinReach,
   JoinTopology,
   PackageIndex,
   SourceContextAuthorize,
   SourceContextEntry,
   SourceContextFilter,
   SourceContextGiven,
} from "./types";
import { parseEnvironmentName, parsePackageName } from "../../service/identity";

/**
 * Caps on doc text carried as CONTEXT alongside a field hit, where the point
 * is the grain caveat, not the model file. A source that is itself a hit
 * carries its doc in full.
 */
export const SOURCE_DOC_MAX_CHARS = 500;
const JOIN_DOC_MAX_CHARS = 200;

export function truncateDoc(doc: string, max: number): string {
   if (doc.length <= max) return doc;
   const cut = doc.slice(0, max);
   const lastSpace = cut.lastIndexOf(" ");
   return `${(lastSpace > max * 0.6 ? cut.slice(0, lastSpace) : cut).trimEnd()}…`;
}

type AnnotationLines = Array<string | { value: string }>;

/** The routes an authored doc is read from, in order: `#(doc)`, then a `#"` doc string. */
const AUTHORED_DOC_ROUTES = ["doc", '"'];

function linesOf(annotations?: AnnotationLines): string[] {
   return (annotations ?? []).map((a) => (typeof a === "string" ? a : a.value));
}

function oneLine(text: string): string {
   return text.replace(/\s+/g, " ").trim();
}

/** `#(doc)` text, else `#"` doc string text, empty when there is neither. */
function authoredDoc(lines: string[]): string {
   for (const route of AUTHORED_DOC_ROUTES) {
      const text = payloadsOnRoute(lines, route)
         .map((payload) => payload.trim())
         .filter(Boolean)
         .join(" ");
      if (text) return text;
   }
   return "";
}

function dbmetaDescription(lines: string[]): string {
   for (const line of lines) {
      if (!isOnRoute(line, "dbmeta")) continue;
      // The tag parser hydrates `@env.` from the server's environment, and this
      // description is served to clients as the entity's doc.
      if (hasEnvReference(line)) {
         warnEnvReferenceDropped(line.slice(0, 80), "dbmeta");
         continue;
      }
      const { tag, log } = parseAnnotation(line);
      if (log.length > 0) continue;
      const description = tag.text("description")?.trim();
      if (description) return description;
   }
   return "";
}

/**
 * The doc text of one entity and where it came from: the authored doc in
 * `annotations`, else the `#(dbmeta)` description in `dbmetaFrom`.
 */
function docParts(
   annotations?: AnnotationLines,
   dbmetaFrom: AnnotationLines | undefined = annotations,
): { text: string; fromDbmeta: boolean } {
   const authored = oneLine(authoredDoc(linesOf(annotations)));
   if (authored) return { text: authored, fromDbmeta: false };
   const description = oneLine(dbmetaDescription(linesOf(dbmetaFrom)));
   return { text: description, fromDbmeta: description !== "" };
}

/**
 * Extract ONLY doc text: `#(doc)`, else a `#"` doc string, else the
 * `#(dbmeta)` description, empty when there is none. The safe input for
 * embedding: unlike docText it never falls back to raw annotation lines, so
 * `#(access_filter)` rules, tenant lists and `#(malloy)` internals never reach
 * an external embedding provider.
 */
export function docOnlyText(annotations?: AnnotationLines): string {
   return docParts(annotations).text;
}

/**
 * Human-facing doc text for the response `doc` field: docOnlyText's text,
 * falling back to the raw annotation lines. Never embedding input (see
 * docOnlyText).
 * Accepts Annotation objects (sources, fields) and raw strings (queries).
 */
export function docText(annotations?: AnnotationLines): string {
   return docOnlyText(annotations) || rawDoc(annotations);
}

/**
 * Every annotation line on one line, the fallback when there is no doc text.
 * A `#(classification)` note has its own card field, so it is not doc text.
 */
function rawDoc(annotations?: AnnotationLines): string {
   return oneLine(
      linesOf(annotations)
         .filter((line) => !isClassificationLine(line))
         .join(" "),
   );
}

/**
 * An entity's `doc`, `embedDoc` and `docSource`, read from `annotations`, with
 * the `#(dbmeta)` fallback read from `dbmetaFrom`.
 */
export function entityDocs(
   annotations?: AnnotationLines,
   dbmetaFrom: AnnotationLines | undefined = annotations,
): Pick<Entity, "doc" | "embedDoc" | "docSource"> {
   const { text, fromDbmeta } = docParts(annotations, dbmetaFrom);
   return {
      doc: text || rawDoc(annotations),
      embedDoc: text,
      ...(fromDbmeta ? { docSource: "dbmeta" as const } : {}),
   };
}

/**
 * The classification an entity reports: the lineage's answer for the field's
 * path when the compiled model is at hand (declared, or inferred from the
 * fields it reads, with those inputs), else the field's own annotation.
 */
function classificationOf(
   lineage: Map<string, FieldClassification> | undefined,
   path: string,
   annotations?: Array<string | { value: string }>,
): Pick<
   Entity,
   "classifications" | "classificationInferred" | "classificationInputs"
> {
   const known = lineage?.get(path);
   if (known) {
      return {
         ...(known.classifications.length > 0
            ? { classifications: known.classifications }
            : {}),
         ...(known.inferred.length > 0
            ? { classificationInferred: true as const }
            : {}),
         ...(known.inputs.length > 0
            ? { classificationInputs: known.inputs }
            : {}),
      };
   }
   const { values } = readClassification(annotations);
   return values.length > 0 ? { classifications: values } : {};
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
 * the whole definition is a reference to another field, by name or dotted path.
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
      // A dotted path is a referent too (`delivery_city is delivery.city`):
      // collapseAliases folds only a source's own fields, so it can match a
      // record member, never a field reached through a join.
      // A member path is spelled as the index names members (memberSegment),
      // so `src is traffic_source.\`source\`` finds its referent.
      const path = field.e?.node === "field" ? (field.e.path ?? []) : [];
      const referent =
         path.length === 0
            ? undefined
            : path.length === 1
              ? path[0]
              : path.map(memberSegment).join(".");
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
   type?: FieldType;
}

/** A field's compiled type, as far as the record walk reads it. */
export interface FieldType {
   kind?: string;
   /**
    * A record's members. A member's annotations carry the database's
    * `#(dbmeta)` description of that nested column, when the connection
    * has `dbmeta` on; the model cannot annotate a member itself.
    */
   fields?: Array<{
      name: string;
      type?: FieldType;
      annotations?: AnnotationLines;
   }>;
   /** An array's element. */
   element_type?: FieldType;
}

/**
 * How deep record members are indexed: `delivery.address.city` is depth 2.
 * Nested records come from the warehouse's schema rather than from joins a
 * modeler wrote, so they get their own cap, above MAX_JOIN_PATH_DEPTH.
 */
const MAX_RECORD_DEPTH = 4;

/** Most record members indexed per source: a wide event table can hold hundreds. */
export const MAX_RECORD_MEMBERS_PER_SOURCE = 300;

/**
 * A member name a dotted path can carry segment by segment: bare, or
 * backticked when it is a reserved word (memberSegment). A STRUCT key can
 * hold a dot or a space (`{'a.b': 1}`): joined raw, `delivery.a.b` would
 * read as a nested record, so such a member is not indexed.
 */
const PLAIN_MEMBER_NAME = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Malloy's reserved words: the keywords its lexer reads without a trailing
 * colon, matched case-insensitively. A member so named (GA4's
 * `traffic_source.source`) is a plain identifier but does not compile bare,
 * only quoted: `` traffic_source.`source` ``. Malloy does not export its
 * keyword list, so this is a copy of it; record_members.spec.ts checks it
 * against the list Malloy generates from its grammar.
 */
export const MALLOY_RESERVED_WORDS: ReadonlySet<string> = MALLOY_BARE_KEYWORDS;

/**
 * A plain name as a segment of a record member's path, backticked when it is
 * reserved: the record's own column (`` `date`.month ``) as well as each
 * member under it.
 */
export function memberSegment(name: string): string {
   return identifier(name);
}

/**
 * The field reference an entity's name spells, one segment per element,
 * without quotes, for a caller that quotes each one: `customer.city`
 * through the join `customer` is `["customer", "city"]`,
 * `` traffic_source.`source` `` is `["traffic_source", "source"]`. The part
 * past the join path is split only for a record member: any other field's
 * name is one name even when it holds a dot. Splitting a member is sound
 * because collectRecordMembers only walks a record whose name, and every
 * member name under it, is a plain identifier, and backticks only a
 * reserved one.
 */
export function fieldPathOf(e: {
   name: string;
   recordPath?: string;
   joinPath?: string;
}): string[] {
   const joins = e.joinPath ? e.joinPath.split(".") : [];
   const rest = e.joinPath ? e.name.slice(e.joinPath.length + 1) : e.name;
   const tail = e.recordPath
      ? rest.split(".").map((segment) => segment.replace(/`/g, ""))
      : [rest];
   return [...joins, ...tail];
}

/**
 * The members of a record dimension (a BigQuery RECORD, a DuckDB STRUCT) as
 * entities of their own, under dotted names (`delivery.city`), so they can be
 * found and grouped by. A member of a source's own record is that source's
 * own field: it carries `recordPath`, never `joinPath`, so the engine does
 * not describe it as reached through a join. A member of a record on a joined
 * source keeps the join's `joinPath` beside its `recordPath`.
 *
 * A member of a repeated record (an array of records, `lines.sku`) has
 * relationship `many`, because grouping by it gives a group per element
 * value; a record inside a repeated record stays `many`. A member takes its
 * record's classification when it has none of its own, and its doc from its own
 * annotations (entityDocs), as any other field does. An array of scalars has
 * one member, its element, named `each` (`tags.each`), also `many`. A member
 * whose name is not a plain identifier (PLAIN_MEMBER_NAME) is not indexed,
 * nor anything under it; one named by a reserved word is backticked
 * (`` traffic_source.`source` ``). At most MAX_RECORD_MEMBERS_PER_SOURCE
 * are indexed per source (`budget`), in schema order; a dropped member is
 * still queryable by name, only not offered by search.
 */
export function collectRecordMembers(args: {
   type: FieldType | undefined;
   /** The member name's dotted prefix: `delivery`, or `customer.profile`. */
   path: string;
   /** The record's path within its source: `delivery`, or `profile`. */
   recordPath: string;
   /** The join the record is reached through, for a joined source's record. */
   joinPath?: string;
   sourceName: string;
   modelPath: string;
   /** The relationship of the path so far: `one` from a plain source. */
   fanout: Relationship;
   /** The join path's own relationship; `fanout` at depth 1 when absent. */
   joinFanout?: Relationship;
   depth: number;
   nextId: () => string;
   out: Entity[];
   lineage?: Map<string, FieldClassification>;
   /** The record's classes, which a member without its own inherits. */
   parentClasses?: Pick<
      Entity,
      "classifications" | "classificationInferred" | "classificationInputs"
   >;
   budget: { left: number };
}): void {
   const { path, recordPath, depth } = args;
   const joinFanout = args.joinFanout ?? args.fanout;
   let type = args.type;
   let fanout = args.fanout;
   if (type?.kind === "array_type") {
      type = type.element_type;
      fanout = "many";
      // An array of scalars has one member, its element, which Malloy names
      // `each` (`tags.each`). Recursing on it as a record of one `each` field
      // indexes it like any other member of a repeated record.
      if (type && type.kind !== "record_type" && type.kind !== "array_type") {
         type = { kind: "record_type", fields: [{ name: "each", type }] };
      }
   }
   if (type?.kind !== "record_type" || depth > MAX_RECORD_DEPTH) return;
   // A record whose own name holds a dot or a space (`a.b`): a member's
   // dotted name under it would not compile, and memberPathOf could not
   // split it back into segments.
   if (depth === 1 && !PLAIN_MEMBER_NAME.test(recordPath.replace(/`/g, "")))
      return;
   for (const member of type.fields ?? []) {
      if (!member.name || !PLAIN_MEMBER_NAME.test(member.name)) continue;
      if (args.budget.left <= 0) return;
      args.budget.left -= 1;
      const segment = memberSegment(member.name);
      const name = `${path}.${segment}`;
      // Lineage is keyed by the unquoted path.
      const own = classificationOf(
         args.lineage,
         `${path}.${member.name}`.replace(/`/g, ""),
         undefined,
      );
      const classes = own.classifications ? own : (args.parentClasses ?? {});
      args.out.push({
         id: args.nextId(),
         kind: "dimension",
         name,
         source: args.sourceName,
         modelPath: args.modelPath,
         ...entityDocs(member.annotations),
         ...classes,
         ...(args.joinPath ? { joinPath: args.joinPath } : {}),
         recordPath,
         // On a source's own record only `many` says anything: `one` is
         // what every own field already is.
         ...(args.joinPath || fanout === "many"
            ? { relationship: fanout }
            : {}),
         ...(args.joinPath && fanout !== joinFanout
            ? { joinRelationship: joinFanout }
            : {}),
         dataType: malloyType(member),
      });
      collectRecordMembers({
         ...args,
         type: member.type,
         path: name,
         recordPath: `${recordPath}.${segment}`,
         fanout,
         joinFanout,
         depth: depth + 1,
         parentClasses: classes,
      });
   }
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
   return splitSourceId(join.sourceID ?? join.referenceID);
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
 * Whether the source a joined record lives on is listed, not deny-all, and
 * holds the record as its own field. Then directEntitiesOf drops the joined
 * copies of its members, which are indexed on that source and reached by
 * route, so walking them here would only spend the shared member budget
 * that a later inline-table join's record needs.
 */
function joinedMembersListed(
   reaches: readonly JoinReach[] | undefined,
   joinPath: string,
   record: string,
   listedFields: ReadonlyMap<string, ReadonlySet<string>>,
   droppedSources: ReadonlySet<string>,
): boolean {
   const reach = reaches?.find((r) => r.path.join(".") === joinPath);
   if (!reach) return false;
   const key = sourceContextKey(reach.targetModelPath, reach.targetSource);
   return (
      !droppedSources.has(key) && (listedFields.get(key)?.has(record) ?? false)
   );
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
   /** The entry source's lineage, keyed by dotted path; absent for a stand-in. */
   lineage?: Map<string, FieldClassification>;
   /** Record members left to index for this source (collectRecordMembers). */
   budget: { left: number };
   /**
    * Whether the source a join reaches lists this record itself, so its
    * members are indexed there and reached by route (joinedMembersListed).
    */
   membersListedElsewhere?: (joinPath: string, record: string) => boolean;
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
      lineage,
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
         ...entityDocs(field.annotations),
         ...classificationOf(
            lineage,
            `${joinPath}.${field.name}`,
            field.annotations,
         ),
         joinPath,
         relationship: fanout,
         ...(field.kind === "measure" || field.kind === "dimension"
            ? { dataType: malloyType(field) }
            : {}),
      });
      if (
         field.kind === "dimension" &&
         !args.membersListedElsewhere?.(joinPath, field.name)
      ) {
         collectRecordMembers({
            type: field.type,
            path: `${joinPath}.${memberSegment(field.name)}`,
            recordPath: memberSegment(field.name),
            joinPath,
            sourceName,
            modelPath,
            fanout,
            depth: 1,
            nextId,
            out,
            ...(lineage ? { lineage } : {}),
            parentClasses: classificationOf(
               lineage,
               `${joinPath}.${field.name}`,
               field.annotations,
            ),
            budget: args.budget,
         });
      }
   }
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
   const sourceLines = new Map<string, string[]>();
   const joinLines = new Map<Entity, string[]>();
   // One reader for the whole walk: several sources share a model file.
   const sourceTextFor = makeSourceTextReader(pkg);
   let n = 0;
   // Each listed source's top-level field names, package-wide, and every
   // deny-all source, both before the walk: a join can name a source from a
   // later file, and joinedMembersListed reads both.
   const listedFields = new Map<string, Set<string>>();
   for (const apiModel of models) {
      const modelPath = apiModel.path;
      const model = modelPath ? pkg.getModel(modelPath) : undefined;
      if (!modelPath || !model) continue;
      for (const apiSource of model.getSources?.() ?? []) {
         if (apiSource.name && isUnconditionalDenyAuthorize(apiSource)) {
            droppedSources.add(sourceContextKey(modelPath, apiSource.name));
         }
      }
      for (const info of model.getSourceInfos() ?? []) {
         listedFields.set(
            sourceContextKey(modelPath, info.name),
            new Set((info.schema?.fields ?? []).map((f) => f.name)),
         );
      }
   }
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

      for (const sourceInfo of sourceInfos) {
         const sourceName = sourceInfo.name;
         // An unconditional `false` denies every caller, so the card offers
         // nothing: drop the source rather than let the agent learn from the
         // 403. Every OTHER gate stays reported, because a caller's givens are
         // untrusted here and evaluating a real rule would be forgeable (see
         // execute_query_tool.ts).
         const governanceKey = sourceContextKey(modelPath, sourceName);
         sourceLines.set(governanceKey, linesOf(sourceInfo.annotations));
         if (droppedSources.has(governanceKey)) {
            continue;
         }
         const provenance = readFieldProvenance(
            modelDef,
            sourceName,
            sourceTextFor,
         );
         // Record members this source may still index (collectRecordMembers).
         // One budget for its own records and its joined sources' records:
         // Malloy lists a source's own fields before its joins, so its own
         // members are counted first and a joined record gets what is left.
         // A joined record its own source lists spends none (see
         // joinedMembersListed).
         const memberBudget = { left: MAX_RECORD_MEMBERS_PER_SOURCE };
         // The source's visible fields: a hidden one (`include { public: ... }`)
         // is not in its schema, so nothing of it is indexed.
         const visible = new Set(
            (
               (
                  sourceInfo as {
                     schema?: { fields?: Array<{ name?: string }> };
                  }
               ).schema?.fields ?? []
            ).map((f) => f.name),
         );
         // What each reachable field is, declared or by lineage. Absent for a
         // stand-in with no compiled model; fields then report their own note.
         const ir = modelDef ? findIrSource(modelDef, sourceName) : undefined;
         const lineage = modelDef
            ? readSourceClassifications(ir?.fields, ir?.query)
            : undefined;
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
         // Undocumented, it takes its table's `#(dbmeta)` description.
         const ownDoc = readSourceOwnDoc(modelDef, sourceName);
         entities.push({
            id: String(n++),
            kind: "source",
            name: sourceName,
            source: sourceName,
            modelPath,
            ...entityDocs(
               ownDoc ?? sourceInfo.annotations,
               sourceInfo.annotations,
            ),
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
               const join: Entity = {
                  id: String(n++),
                  kind: "join",
                  name: field.name,
                  // The source that DECLARES the join, so a drill-down on it
                  // sees the join.
                  source: sourceName,
                  modelPath,
                  ...entityDocs(field.annotations),
                  relationship: field.relationship,
                  ...joinTargetOf(topology.get(governanceKey), field.name),
               };
               entities.push(join);
               joinLines.set(join, linesOf(field.annotations));
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
                  lineage,
                  budget: memberBudget,
                  membersListedElsewhere: (joinPath, record) =>
                     joinedMembersListed(
                        topology.get(governanceKey),
                        joinPath,
                        record,
                        listedFields,
                        droppedSources,
                     ),
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
               ...entityDocs(field.annotations),
               ...(field.kind === "view"
                  ? {}
                  : {
                       dataType: malloyType(field),
                       ...classificationOf(
                          lineage,
                          field.name,
                          field.annotations,
                       ),
                    }),
               ...(fieldProvenance?.code ? { code: fieldProvenance.code } : {}),
               ...(!isView && fieldProvenance?.aliasOf
                  ? { aliasOf: fieldProvenance.aliasOf }
                  : {}),
            });
            // A dimension defined as another (`addr is delivery`) is a
            // second spelling of that record: its members are listed once,
            // under the record's own name. Unless that record is hidden
            // (renamed, then `include { public: ship_to }`): then the alias
            // is the only spelling, and lists them.
            // A nested record (`addr is delivery.address`) is listed under
            // its top-level record's name when that record is visible.
            const aliasOf = fieldProvenance?.aliasOf;
            const aliasOfVisible =
               aliasOf !== undefined &&
               (visible.has(aliasOf) ||
                  (aliasOf.includes(".") &&
                     visible.has(aliasOf.split(".")[0].replace(/`/g, ""))));
            if (field.kind === "dimension" && !aliasOfVisible) {
               collectRecordMembers({
                  type: (field as { type?: FieldType }).type,
                  path: memberSegment(field.name),
                  recordPath: memberSegment(field.name),
                  sourceName,
                  modelPath,
                  fanout: "one",
                  depth: 1,
                  nextId: () => String(n++),
                  out: entities,
                  ...(lineage ? { lineage } : {}),
                  parentClasses: classificationOf(
                     lineage,
                     field.name,
                     field.annotations,
                  ),
                  budget: memberBudget,
               });
            }
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
            ...entityDocs(query.annotations),
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
   // Nothing a deny-all source documents reaches a prompt through a join to
   // it. A join that documents nothing carries its target's notes, so the join
   // keeps only lines its target does not have (none, when the target's are
   // unknown), and every field reached through it loses its doc. After the
   // full walk, since a join can name a source from a later file.
   for (const e of deduped) {
      const path =
         e.kind === "join" ? [e.name] : (e.joinPath?.split(".") ?? []);
      if (path.length === 0) continue;
      const locked = lockedReach(
         topology.get(sourceContextKey(e.modelPath, e.source ?? "")),
         path,
         droppedSources,
      );
      if (!locked) continue;
      const targetLines = sourceLines.get(
         sourceContextKey(locked.targetModelPath, locked.targetSource),
      );
      const ownLines =
         e.kind === "join" && locked.path.length === path.length && targetLines
            ? (joinLines.get(e) ?? []).filter(
                 (line) => !targetLines.includes(line),
              )
            : [];
      delete e.docSource;
      Object.assign(e, entityDocs(ownLines, []));
   }
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
   const collapsed = inheritRecordClasses(collapseAliases(deduped));
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

/** The first deny-all source on a join path, as the reach that leads to it. */
function lockedReach(
   reaches: readonly JoinReach[] | undefined,
   path: readonly string[],
   droppedSources: ReadonlySet<string>,
): JoinReach | undefined {
   return reaches
      ?.filter(
         (reach) =>
            reach.path.length <= path.length &&
            reach.path.every((alias, i) => path[i] === alias) &&
            droppedSources.has(
               sourceContextKey(reach.targetModelPath, reach.targetSource),
            ),
      )
      .sort((a, b) => a.path.length - b.path.length)[0];
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
 * Give each record member without a class of its own its record's class,
 * after collapseAliases. The class belongs to the column, so it is read off
 * the record's whole alias group: `#(classification) "pci" dimension:
 * billing_details is billing` classifies `billing.card`, though the members
 * are listed under the raw record's name.
 */
function inheritRecordClasses(entities: Entity[]): Entity[] {
   // Backticks dropped: a record is listed as `date`, while its members'
   // recordPath spells it `` `date` `` (memberSegment).
   const cardKey = (e: Entity, name: string) =>
      [
         e.modelPath,
         e.source ?? "",
         e.joinPath ?? "",
         name.replace(/`/g, ""),
      ].join(KEY_SEPARATOR);
   const classesOf = new Map<string, Entity>();
   for (const e of entities) {
      if (e.kind !== "dimension" || !e.classifications) continue;
      for (const name of [e.name, ...(e.aliases ?? [])]) {
         classesOf.set(cardKey(e, name), e);
      }
   }
   if (classesOf.size === 0) return entities;
   for (const e of entities) {
      if (!e.recordPath || e.classifications) continue;
      // The nearest classified record on the member's path.
      const parts = e.recordPath.split(".");
      for (let i = parts.length; i >= 1; i--) {
         const record = parts.slice(0, i).join(".");
         const name = e.joinPath ? `${e.joinPath}.${record}` : record;
         const donor = classesOf.get(cardKey(e, name));
         if (!donor) continue;
         e.classifications = donor.classifications;
         if (donor.classificationInferred) e.classificationInferred = true;
         if (donor.classificationInputs) {
            e.classificationInputs = donor.classificationInputs;
         }
         break;
      }
   }
   return entities;
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
            if (donor.docSource) keep.docSource = donor.docSource;
         }
      }
      // A class belongs to the column, so a spelling's class is the group's:
      // `state is st` classified and `raw_state is st` not are one column
      // with one class, whichever spelling survives.
      if (!keep.classifications) {
         const donor = members.find((e) => e !== keep && e.classifications);
         if (donor) {
            keep.classifications = donor.classifications;
            if (donor.classificationInferred)
               keep.classificationInferred = true;
            if (donor.classificationInputs) {
               keep.classificationInputs = donor.classificationInputs;
            }
         }
      }
      // A repeated record's member is `many`, and so is a dimension
      // declared over it (`qty_each is lines.qty`): grouping by either
      // gives a group per element.
      if (!keep.relationship) {
         const donor = members.find((e) => e !== keep && e.relationship);
         if (donor?.relationship) keep.relationship = donor.relationship;
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
function entityCardKey(e: {
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
 * A doc reduced to a catalog label: its first sentence or line (blank lines
 * before it skipped), capped. Used for a source's card and for a package in
 * a get_context listing, so one text gets one summary. Undocumented sources
 * get nothing rather than a fabricated summary.
 */
export function oneLineSummary(doc: string): string | undefined {
   const firstLine = doc.trim().split("\n")[0]?.trim();
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
         ...(e.docSource ? { docSource: e.docSource } : {}),
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

/**
 * Each package's entity index, by Package instance: a reload swaps in a new
 * instance, so a stale entry drops out of the WeakMap on its own.
 */
export type PackageIndexCache = WeakMap<Package, PackageIndex>;

/** Get, or lazily build and cache in `indexes`, the entity index for a package. */
export async function getPackageIndex(
   indexes: PackageIndexCache,
   environmentStore: PackageLookup,
   environmentName: string,
   packageName: string,
): Promise<PackageIndex> {
   const environment = await environmentStore.getEnvironment(
      parseEnvironmentName(environmentName),
      false,
   );
   const pkg = await environment.getPackage(
      parsePackageName(packageName),
      false,
   );
   return packageIndexOf(indexes, pkg, packageName);
}

/** The entity index for `pkg` itself, built once per Package instance. */
export async function packageIndexOf(
   indexes: PackageIndexCache,
   pkg: Package,
   packageName: string,
): Promise<PackageIndex> {
   const cached = indexes.get(pkg);
   if (cached) return cached;

   const collected = await collectEntities(pkg);
   const { entities } = collected;
   const byId = new Map(entities.map((e) => [e.id, e]));
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
      entityCount: entities.length,
      sourceContext: buildSourceContext(collected),
      droppedSources: collected.droppedSources,
   };
   indexes.set(pkg, built);
   logger.debug("[MCP Tool getContext] Built and cached entity index", {
      packageName,
      entityCount: entities.length,
   });
   return built;
}
