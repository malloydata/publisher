// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Finding the joins a CALLER wrote in a compiled query, and resolving each to
 * the model-declared sources it reaches.
 *
 * A caller join is held to the joined source's gate and to the query boundary
 * as if it were an extra run target (`docs/authorize.md`). Everything here
 * decides on compiler-set facts the caller cannot write: a join's `location`
 * (the author's only when its URL is one of the on-disk model's own files) and
 * its `sourceID` (`name@declaring-url`). A join struct's annotations are never
 * read — an annotated join REPLACES them — and caller text is read only where
 * the IR keeps no link at all (an inline `x extend { … }`, and the derivation
 * chain of a caller-declared source).
 *
 * Model-state-free on purpose, so `Model` supplies what only it knows (which
 * names the on-disk model declares) through {@link CallerJoinContext}.
 */

import { isSourceDef, type ModelDef } from "@malloydata/malloy";
import {
   buildDerivationBaseMap,
   buildIsEdgeMap,
   buildJoinBaseMap,
} from "./query_text";

/**
 * Where caller-written text sits in a compiled query.
 *
 * `query`: the ad-hoc query path, where the caller's text is the whole query
 * and compiles under its own `internal://query/<uuid>` URL. `span`: `/compile`
 * append scope, where the caller's text is appended to the model's in one
 * virtual file and starts at 0-based line `fromLine`.
 */
export type CallerRegion =
   | { kind: "query"; text: string }
   | { kind: "span"; url: string; fromLine: number; text: string };

/** A deterministic locator into a prepared query; see {@link locateCallerJoin}. */
export type CallerJoinPath = ReadonlyArray<string | number>;

/** A join field found by {@link collectCallerJoins}. */
export interface CallerJoin {
   join: IrStruct;
   alias: string;
   path: CallerJoinPath;
}

export type CallerJoinResolution =
   | {
        kind: "resolved";
        /** Every model-declared source whose gates the join must satisfy. */
        names: Set<string>;
        /**
         * The names the query boundary checks: a composite's own name rather
         * than its members, the same rule a composite run target gets.
         */
        boundaryNames: Set<string>;
        /** An author named query the join reads unmodified, which the boundary may curate. */
        curatedQuery?: string;
        composite: boolean;
        /**
         * Caller-declared sources the join reads, gated from their compiled
         * struct as a request-declared run target is, with the text chain as
         * the laundering proof.
         */
        callerSources: CallerSource[];
        /** Where the text chain the boundary needs could not be established. */
        boundaryUnprovenAt?: string;
     }
   | { kind: "unproven"; at: string };

/** A caller-declared `contents` entry a caller join reaches. */
export interface CallerSource {
   key: string;
   struct: IrStruct;
   chain: ReturnType<typeof derivationTerminals>;
}

/** Thrown for every "cannot tell"; callers turn it into a refusal. */
export class CallerJoinWalkError extends Error {}

/** Caller joins nested deeper than this are refused rather than walked. */
export const CALLER_JOIN_MAX_DEPTH = 16;
/** More caller joins than this in one query are refused rather than walked. */
export const CALLER_JOIN_MAX_COUNT = 256;
/** Objects the completeness scan visits before it gives up and refuses. */
const IR_SCAN_MAX_NODES = 200_000;
/** Budget for one derivation-chain walk over caller text. */
const DERIVATION_CHAIN_MAX_NAMES = 64;
/** Depth bound for resolving a join's own nested structs. */
const RESOLVE_MAX_DEPTH = 8;

interface IrLocation {
   url?: string;
   range?: { start?: { line?: number } };
}

/** The duck type of the IR this module reads; Malloy exports no such type. */
export interface IrStruct {
   type?: string;
   name?: string;
   as?: string;
   join?: string;
   sourceID?: string;
   location?: IrLocation;
   fields?: IrStruct[];
   pipeline?: IrSegment[];
   sources?: IrStruct[];
   query?: IrQuery;
   filterList?: { code?: string }[];
}

interface IrSegment {
   extendSource?: IrStruct[];
   queryFields?: IrStruct[];
}

interface IrQuery {
   name?: string;
   location?: IrLocation;
   structRef?: IrStruct | string;
   pipeline?: IrSegment[];
   compositeResolvedSourceDef?: IrStruct;
}

export interface PreparedQueryIr {
   _query?: IrQuery;
   _modelDef?: ModelDef;
}

/** What only `Model` knows about the on-disk model. */
export interface CallerJoinContext {
   /** The on-disk model declares a source by this name. */
   isModelSource(name: string): boolean;
   /** The on-disk model declares a named query by this name. */
   isModelQuery(name: string): boolean;
   /** The URLs of the on-disk model's own files: it and its imports. */
   authorUrls: ReadonlySet<string>;
}

/**
 * Whether `location` is inside caller-written text. Fails closed: a location
 * is the author's only when its URL is one of the on-disk model's own files
 * (or, for a `span`, a line above the caller's), so an unknown URL or a
 * missing location reads as the caller's.
 */
export function isCallerAuthored(
   location: IrLocation | undefined,
   region: CallerRegion,
   authorUrls: ReadonlySet<string>,
): boolean {
   const url = location?.url;
   if (url === undefined) return true;
   if (region.kind === "span" && url === region.url) {
      const line = location?.range?.start?.line;
      return !(typeof line === "number" && line < region.fromLine);
   }
   return !authorUrls.has(url);
}

/** A join to a source; a record or array is `join`-typed too and is not one. */
function isSourceJoin(field: IrStruct): boolean {
   return !!field.join && isSourceDef(field as never);
}

function aliasOf(field: IrStruct): string {
   return field.as ?? field.name ?? "";
}

function contentsStruct(
   modelDef: ModelDef | undefined,
   name: string,
): IrStruct | undefined {
   const entry = modelDef?.contents[name];
   return entry && isSourceDef(entry)
      ? (entry as unknown as IrStruct)
      : undefined;
}

/** The run target the walk starts from: `structRef`, name or object. */
function runTargetStruct(prepared: PreparedQueryIr): IrStruct | undefined {
   const ref = prepared._query?.structRef;
   if (typeof ref === "string") return contentsStruct(prepared._modelDef, ref);
   return ref && typeof ref === "object" ? ref : undefined;
}

/**
 * Every join written in caller text that the LIVE compiled query reaches, with
 * a path that finds the same join in a recompile of the same text.
 *
 * One walker runs at every level: a struct's fields and a pipeline stage's
 * `extendSource`, recursing through caller joins and caller-located turtles
 * (views, nests), a query source's own query, and a composite's members. Roots
 * are the run target, its `compositeResolvedSourceDef` (the struct SQL is
 * generated from), and the query's pipeline. An author join is not recursed
 * into, which keeps the author rule and keeps a deep author graph off the
 * depth bound. Throws {@link CallerJoinWalkError} past the depth or count
 * bound, for a join with no location, and when a completeness scan finds a
 * caller join the walk did not place.
 */
export function collectCallerJoins(
   prepared: PreparedQueryIr,
   region: CallerRegion,
   authorUrls: ReadonlySet<string>,
): CallerJoin[] {
   const query = prepared._query;
   if (!query) throw new CallerJoinWalkError("no compiled query to walk");
   const isCaller = (location: IrLocation | undefined) =>
      isCallerAuthored(location, region, authorUrls);
   const modelDef = prepared._modelDef;
   const found: CallerJoin[] = [];

   const record = (join: IrStruct, path: CallerJoinPath, depth: number) => {
      if (depth >= CALLER_JOIN_MAX_DEPTH) {
         throw new CallerJoinWalkError(
            "caller joins nest past the depth bound",
         );
      }
      if (found.length >= CALLER_JOIN_MAX_COUNT) {
         throw new CallerJoinWalkError("too many caller joins");
      }
      found.push({ join, alias: aliasOf(join), path });
      walkStruct(join, path, depth + 1);
   };

   const walkFields = (
      fields: IrStruct[] | undefined,
      path: CallerJoinPath,
      key: "fields" | "extendSource",
      depth: number,
      // A composite's field list is synthesized from its members, and its
      // joins carry no location; the members themselves are walked instead.
      synthesized: boolean,
   ): void => {
      for (const field of fields ?? []) {
         const fieldPath = [...path, key, aliasOf(field)];
         if (isSourceJoin(field)) {
            if (field.location?.url === undefined) {
               if (synthesized) continue;
               throw new CallerJoinWalkError("a join carries no location");
            }
            if (isCaller(field.location)) record(field, fieldPath, depth);
         } else if (field.type === "turtle" && isCaller(field.location)) {
            walkPipeline(field.pipeline, [...fieldPath, "pipeline"], depth);
         }
      }
   };

   const walkStruct = (
      struct: IrStruct,
      path: CallerJoinPath,
      depth: number,
   ): void => {
      const composite = struct.type === "composite";
      walkFields(struct.fields, path, "fields", depth, composite);
      if (composite) {
         for (const [i, member] of (struct.sources ?? []).entries()) {
            walkStruct(member, [...path, "sources", i], depth);
         }
      }
      if (struct.type === "query_source") {
         walkQuery(struct.query, [...path, "query"], depth);
      }
   };

   const walkQuery = (
      q: IrQuery | undefined,
      path: CallerJoinPath,
      depth: number,
   ): void => {
      if (!q) return;
      const ref = q.structRef;
      // A string names a `contents` entry; only a caller-declared one can hold
      // caller joins.
      const base =
         typeof ref === "string" ? contentsStruct(modelDef, ref) : ref;
      if (base && (typeof ref !== "string" || isCaller(base.location))) {
         walkStruct(base, [...path, "structRef"], depth);
      }
      if (q.compositeResolvedSourceDef) {
         walkStruct(q.compositeResolvedSourceDef, [...path, "resolved"], depth);
      }
      walkPipeline(q.pipeline, [...path, "pipeline"], depth);
   };

   const walkPipeline = (
      pipeline: IrSegment[] | undefined,
      path: CallerJoinPath,
      depth: number,
   ): void => {
      for (const [i, segment] of (pipeline ?? []).entries()) {
         walkFields(
            segment.extendSource,
            [...path, i],
            "extendSource",
            depth,
            false,
         );
         for (const field of segment.queryFields ?? []) {
            if (field.type === "turtle") {
               walkPipeline(
                  field.pipeline,
                  [...path, i, "queryFields", aliasOf(field), "pipeline"],
                  depth,
               );
            }
         }
      }
   };

   const target = runTargetStruct(prepared);
   if (target) walkStruct(target, ["target"], 0);
   const resolved = query.compositeResolvedSourceDef;
   if (resolved) {
      assertResolvedJoinsAccountedFor(resolved, target, isCaller);
      walkStruct(resolved, ["resolved"], 0);
   }
   walkPipeline(query.pipeline, ["pipeline"], 0);

   assertEveryCallerJoinPlaced(prepared, found, isCaller);
   return found;
}

/**
 * Malloy re-resolves a query touching a composite into
 * `compositeResolvedSourceDef`, and a join it copies there can lose its caller
 * location (a caller-joined composite reappears as its member, under the
 * member's file URL). So a join there that does not read as caller-written must
 * be one the declared run target also has; one with no counterpart cannot be
 * placed on either side of the line.
 */
function assertResolvedJoinsAccountedFor(
   resolved: IrStruct,
   target: IrStruct | undefined,
   isCaller: (location: IrLocation | undefined) => boolean,
): void {
   const declared = new Set(
      (target?.fields ?? []).filter(isSourceJoin).map((f) => aliasOf(f)),
   );
   for (const field of resolved.fields ?? []) {
      if (!isSourceJoin(field) || isCaller(field.location)) continue;
      if (!declared.has(aliasOf(field))) {
         throw new CallerJoinWalkError(
            "a resolved composite carries a join the declared source does not",
         );
      }
   }
}

/**
 * The walk above reads named places in the IR; this is the backstop for a
 * place it does not know. Scans the whole compiled query (the run target
 * included) and refuses if any caller-authored join object is not one the walk
 * recorded.
 */
function assertEveryCallerJoinPlaced(
   prepared: PreparedQueryIr,
   found: readonly CallerJoin[],
   isCaller: (location: IrLocation | undefined) => boolean,
): void {
   const placed = new Set<unknown>(found.map((f) => f.join));
   const seen = new Set<unknown>();
   let budget = IR_SCAN_MAX_NODES;
   const stack: unknown[] = [prepared._query, runTargetStruct(prepared)];
   const modelDef = prepared._modelDef;
   while (stack.length > 0) {
      const value = stack.pop();
      if (!value || typeof value !== "object" || seen.has(value)) continue;
      seen.add(value);
      if (--budget < 0) {
         throw new CallerJoinWalkError("compiled query too large to scan");
      }
      if (Array.isArray(value)) {
         for (const item of value) stack.push(item);
         continue;
      }
      const node = value as IrStruct & Record<string, unknown>;
      if (isSourceJoin(node)) {
         // Nothing caller-written sits beneath an author join.
         if (!isCaller(node.location)) continue;
         if (!placed.has(node)) {
            throw new CallerJoinWalkError(
               "a caller join sits where the walk does not look",
            );
         }
      }
      for (const [key, child] of Object.entries(node)) {
         // `outputStruct` is a stage's result schema, where a nest is a
         // `join`-typed record: nothing there reads a source.
         if (
            key === "location" ||
            key === "annotations" ||
            key === "outputStruct"
         ) {
            continue;
         }
         // A composite's fields are synthesized from its members, unlocated;
         // the members are scanned instead, as the walk does.
         if (key === "fields" && node.type === "composite") continue;
         // A caller-declared source named by string holds its joins in `contents`.
         if (key === "structRef" && typeof child === "string") {
            const named = contentsStruct(modelDef, child);
            if (named && isCaller(named.location)) stack.push(named);
            continue;
         }
         stack.push(child);
      }
   }
}

/**
 * Follow `start` through the caller's own `source:` / `query:` declarations to
 * model-declared names. `proven: false` for any name that is neither a model
 * source nor a declaration the scan could read, or past the name budget.
 */
export function derivationTerminals(
   start: string,
   basesOf: Map<string, Set<string>>,
   isModelSource: (name: string) => boolean,
   maxNames: number = DERIVATION_CHAIN_MAX_NAMES,
): { proven: true; terminals: string[] } | { proven: false; at: string } {
   const seen = new Set<string>();
   const terminals: string[] = [];
   const worklist = [start];
   for (let i = 0; i < worklist.length; i++) {
      const name = worklist[i];
      if (seen.has(name)) continue;
      seen.add(name);
      if (seen.size > maxNames) return { proven: false, at: name };
      // A model-declared source ends the branch; the author's own derivations
      // are not followed, which is the author rule.
      if (isModelSource(name)) {
         terminals.push(name);
         continue;
      }
      const bases = basesOf.get(name);
      if (!bases || bases.size === 0) return { proven: false, at: name };
      for (const base of bases) worklist.push(base);
   }
   return { proven: true, terminals };
}

/**
 * Resolve one caller join to every model-declared source name it reaches.
 *
 * Identity is by `sourceID` against the compiled `contents`, never by name and
 * never by parsing the id (a URL may contain `@`). A matched entry the caller
 * declared comes back as a {@link CallerSource}, to be gated from its compiled
 * struct; one the model declared must be a name the on-disk model declares. A
 * query-source join also resolves its base; a composite join resolves its own
 * id AND every member. Only the join itself may fall back to text when it has
 * no `sourceID` (an inline `extend`): every base any `ALIAS is BASE` spelling
 * gives it must then prove out.
 */
export function resolveCallerJoin(
   callerJoin: CallerJoin,
   compiledModelDef: ModelDef,
   region: CallerRegion,
   context: CallerJoinContext,
): CallerJoinResolution {
   const names = new Set<string>();
   const boundaryNames = new Set<string>();
   const callerSources: CallerSource[] = [];
   let composite = false;
   let curatedQuery: string | undefined;
   let unprovenAt: string | undefined;
   let boundaryUnprovenAt: string | undefined;
   const isCaller = (location: IrLocation | undefined) =>
      isCallerAuthored(location, region, context.authorUrls);
   const text = regionMaps(region);

   const fail = (at: string): false => {
      unprovenAt ??= at;
      return false;
   };

   const add = (name: string, forBoundary: boolean) => {
      names.add(name);
      if (forBoundary) boundaryNames.add(name);
   };

   const chainOf = (start: string) =>
      derivationTerminals(start, text.derivations(), (name) =>
         context.isModelSource(name),
      );

   // Keyed like the on-disk gate map: by the entry's own `as ?? name`.
   const byContentsKey = (key: string, forBoundary: boolean): boolean => {
      const entry = contentsStruct(compiledModelDef, key);
      if (entry && isCaller(entry.location)) {
         // Gated from its struct by the caller; the boundary has only text.
         const chain = chainOf(key);
         callerSources.push({ key, struct: entry, chain });
         if (forBoundary) {
            if (chain.proven) {
               for (const name of chain.terminals) boundaryNames.add(name);
            } else {
               boundaryUnprovenAt ??= chain.at;
            }
         }
         return true;
      }
      const name = entry ? aliasOf(entry) : key;
      if (context.isModelSource(name)) {
         add(name, forBoundary);
         return true;
      }
      if (entry) return fail(name);
      // A name no source entry holds, such as a caller `query:`.
      const chain = chainOf(key);
      if (!chain.proven) return fail(chain.at);
      for (const terminal of chain.terminals) add(terminal, forBoundary);
      return true;
   };

   // The author's query exactly as declared: a refinement keeps its name.
   const isUnmodifiedModelQuery = (q: IrQuery): boolean => {
      if (!q.name || !context.isModelQuery(q.name)) return false;
      const declared = compiledModelDef.contents[q.name] as
         | (IrQuery & { type?: string })
         | undefined;
      return (
         declared?.type === "query" &&
         !isCaller(declared.location) &&
         JSON.stringify(q.pipeline) === JSON.stringify(declared.pipeline)
      );
   };

   const bySourceID = (sourceID: string, forBoundary: boolean): boolean => {
      const keys = Object.entries(compiledModelDef.contents)
         .filter(
            ([, value]) =>
               isSourceDef(value) &&
               (value as unknown as IrStruct).sourceID === sourceID,
         )
         .map(([key]) => key);
      if (keys.length === 0) return fail(sourceID);
      return keys.every((key) => byContentsKey(key, forBoundary));
   };

   const resolveStruct = (
      struct: IrStruct,
      isJoinItself: boolean,
      forBoundary: boolean,
      depth: number,
   ): boolean => {
      if (depth > RESOLVE_MAX_DEPTH) return fail(callerJoin.alias);
      let anchored = false;
      if (struct.sourceID) {
         if (!bySourceID(struct.sourceID, forBoundary)) return false;
         anchored = true;
      }
      if (struct.type === "composite") {
         composite = true;
         const members = struct.sources;
         if (!Array.isArray(members) || members.length === 0) {
            return fail(callerJoin.alias);
         }
         // Members answer to the boundary only when the composite has no
         // declared name of its own to be curated under.
         const membersForBoundary = forBoundary && !struct.sourceID;
         for (const member of members) {
            if (!resolveStruct(member, false, membersForBoundary, depth + 1)) {
               return false;
            }
         }
         anchored = true;
      }
      if (struct.type === "query_source") {
         const q = struct.query;
         if (isJoinItself && q && isUnmodifiedModelQuery(q)) {
            curatedQuery = q.name;
         }
         const ref = q?.structRef;
         if (typeof ref === "string") {
            if (!byContentsKey(ref, forBoundary)) return false;
         } else if (ref && typeof ref === "object") {
            if (!resolveStruct(ref, false, forBoundary, depth + 1))
               return false;
         } else {
            return fail(callerJoin.alias);
         }
         anchored = true;
      }
      if (anchored) return true;
      if (!isJoinItself) return fail(callerJoin.alias);
      const bases = new Set([
         ...(text.joins().get(callerJoin.alias) ?? []),
         ...(text.isEdges().get(callerJoin.alias) ?? []),
      ]);
      if (bases.size === 0) return fail(callerJoin.alias);
      for (const base of bases) {
         if (!byContentsKey(base, forBoundary)) return false;
      }
      return true;
   };

   if (!resolveStruct(callerJoin.join, true, true, 0)) {
      return { kind: "unproven", at: unprovenAt ?? callerJoin.alias };
   }
   return {
      kind: "resolved",
      names,
      boundaryNames,
      curatedQuery,
      composite,
      callerSources,
      boundaryUnprovenAt,
   };
}

interface RegionMaps {
   derivations(): Map<string, Set<string>>;
   joins(): Map<string, Set<string>>;
   isEdges(): Map<string, Set<string>>;
}

const regionMapsCache = new WeakMap<CallerRegion, RegionMaps>();

/** The region's text maps, each built at most once however many joins read it. */
function regionMaps(region: CallerRegion): RegionMaps {
   let maps = regionMapsCache.get(region);
   if (!maps) {
      const once = <T>(build: () => T): (() => T) => {
         let value: T | undefined;
         return () => (value ??= build());
      };
      maps = {
         derivations: once(() => buildDerivationBaseMap(region.text)),
         joins: once(() => buildJoinBaseMap(region.text)),
         isEdges: once(() => buildIsEdgeMap(region.text)),
      };
      regionMapsCache.set(region, maps);
   }
   return maps;
}

/**
 * Find the join at `path` in a prepared query — the same join
 * {@link collectCallerJoins} recorded, in a recompile of the same text. A
 * `target` path starts where Malloy generates SQL from
 * (`compositeResolvedSourceDef ?? structRef`), so a graft is proven on the
 * struct that runs. `undefined` for anything that does not resolve uniquely.
 */
export function locateCallerJoin(
   prepared: PreparedQueryIr,
   path: CallerJoinPath,
): IrStruct | undefined {
   const query = prepared._query;
   const modelDef = prepared._modelDef;
   const resolve = (ref: IrStruct | string | undefined) =>
      typeof ref === "string" ? contentsStruct(modelDef, ref) : ref;
   const byAlias = (list: IrStruct[] | undefined, alias: unknown) => {
      const matches = (list ?? []).filter((f) => aliasOf(f) === alias);
      return matches.length === 1 ? matches[0] : undefined;
   };
   let node: unknown;
   if (path[0] === "target") {
      node = resolve(query?.compositeResolvedSourceDef ?? query?.structRef);
   } else if (path[0] === "resolved") {
      node = query?.compositeResolvedSourceDef;
   } else if (path[0] === "pipeline") {
      node = query?.pipeline;
   } else {
      return undefined;
   }
   let i = 1;
   while (i < path.length && node !== undefined && node !== null) {
      const step = path[i];
      if (typeof step === "number") {
         node = Array.isArray(node) ? node[step] : undefined;
         i += 1;
         continue;
      }
      const current = node as IrStruct & IrSegment & IrQuery;
      switch (step) {
         case "fields":
         case "extendSource":
         case "queryFields":
            node = byAlias(current[step], path[i + 1]);
            i += 2;
            break;
         case "pipeline":
         case "query":
         case "sources":
            node = current[step];
            i += 1;
            break;
         case "structRef":
            node = resolve(current.structRef);
            i += 1;
            break;
         case "resolved":
            node = current.compositeResolvedSourceDef;
            i += 1;
            break;
         default:
            return undefined;
      }
   }
   const found = node as IrStruct | undefined;
   return found && typeof found === "object" && isSourceJoin(found)
      ? found
      : undefined;
}
