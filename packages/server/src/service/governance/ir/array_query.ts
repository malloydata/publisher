// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Malloy's experimental query on an array (`items -> { aggregate: price.sum() }`,
 * enabled per model by `##! experimental.array_queries`, Malloy 0.0.438): an
 * `arrayQuery` expression node whose `kids.array` is the array the row holds
 * and whose `pipeline` runs on that array's elements. Two readings of it live
 * here, so every IR walker reads it one way:
 *
 *  - {@link forEachFieldPath}: the field paths an expression reads. A path
 *    inside the pipeline is relative to the array's elements (`price`, not
 *    `items.price`), so it is re-rooted under the array's path, as Malloy's
 *    own `fieldUsage` does (`inPlaceDepth`). A walker that descends into the
 *    pipeline as if it were the row's reads a field of the row that the query
 *    never named, or one that does not exist.
 *  - {@link arrayQueryJoins}: the joins a pipeline declares in an `extend:`.
 *    The elements' query can join any source of the model, and Malloy's
 *    `fieldUsage` keeps only the reads of the elements, so neither its summary
 *    nor Publisher's join walks follow such a join to the source it reads.
 *    A model that governs access refuses one ({@link governsAccess}).
 *
 * Pure and SDK-type-only.
 */

import { isSourceDef } from "@malloydata/malloy";
import { isOnRoute } from "../../annotation_reader";

/** The routes whose checks read what a query reads, or which source it reaches. */
const GOVERNING_ROUTES = [
   "authorize",
   "access_filter",
   "carries",
   "classification",
] as const;

/** Options for {@link forEachFieldPath}, one per walker's existing rules. */
export interface FieldPathWalk {
   /** Keys never descended into: locations, function templates. */
   skipKeys: ReadonlySet<string>;
   /** Also read `{type: "fieldref", path}`, a query field naming a field. */
   fieldrefs?: boolean;
   /** Deepest object nesting walked. */
   maxDepth?: number;
}

type IrObject = Record<string, unknown>;

function isObject(v: unknown): v is IrObject {
   return v !== null && typeof v === "object";
}

function stringPath(v: unknown): string[] | undefined {
   return Array.isArray(v) && v.every((p) => typeof p === "string")
      ? (v as string[])
      : undefined;
}

/**
 * The parts of an `arrayQuery` node: the array's path, relative to where the
 * node sits, and the first stage of its pipeline, which reads the elements. A
 * later stage reads the stage before it, not the array. Undefined for any
 * other node, and `arrayPath` undefined when the array is not a plain field.
 */
function arrayQueryParts(
   obj: IrObject,
): { arrayPath?: string[]; firstStage?: unknown } | undefined {
   if (obj.node !== "arrayQuery") return undefined;
   const array = isObject(obj.kids) ? obj.kids.array : undefined;
   const arrayPath =
      isObject(array) && array.node === "field"
         ? stringPath(array.path)
         : undefined;
   const pipeline = Array.isArray(obj.pipeline) ? obj.pipeline : [];
   return { arrayPath, firstStage: pipeline[0] };
}

/**
 * The names a stage's `extend:` gives the elements (a dimension the query on
 * the array declares). A reference to one is not a read of the array; its
 * definition, walked in place, is. A join is left out: what it reads is not
 * the array's, and its path stays one that resolves to nothing.
 */
function elementDeclaredNames(stage: unknown): Set<string> {
   const names = new Set<string>();
   const extend = isObject(stage) ? stage.extendSource : undefined;
   for (const f of Array.isArray(extend) ? extend : []) {
      if (!isObject(f) || (f.join && isSourceDef(f as never))) continue;
      const name = typeof f.as === "string" ? f.as : f.name;
      if (typeof name === "string") names.add(name);
   }
   return names;
}

/**
 * Call `visit` with every field path `e` reads, as a path from the row `e` is
 * evaluated on. A path inside an array query is re-rooted under the array's
 * path (see the module doc); a nested one under its own array in turn.
 */
export function forEachFieldPath(
   e: unknown,
   visit: (path: string[]) => void,
   options: FieldPathWalk,
): void {
   const maxDepth = options.maxDepth ?? Number.POSITIVE_INFINITY;
   // Per root prefix: a node the compiler shares between two branches is read
   // once under each array it sits in.
   const seen = new Map<string, Set<object>>();
   const walk = (
      v: unknown,
      prefix: readonly string[],
      declared: ReadonlySet<string>,
      depth: number,
   ): void => {
      if (!isObject(v) || depth > maxDepth) return;
      const key = prefix.join("\u0000");
      let at = seen.get(key);
      if (!at) seen.set(key, (at = new Set()));
      if (at.has(v)) return;
      at.add(v);
      if (Array.isArray(v)) {
         for (const item of v) walk(item, prefix, declared, depth + 1);
         return;
      }
      const path =
         v.node === "field" || (options.fieldrefs && v.type === "fieldref")
            ? stringPath(v.path)
            : undefined;
      if (path && !(path.length > 0 && declared.has(path[0]))) {
         visit([...prefix, ...path]);
      }
      const parts = arrayQueryParts(v);
      if (parts?.arrayPath) {
         walk(v.kids, prefix, declared, depth + 1);
         walk(
            parts.firstStage,
            [...prefix, ...parts.arrayPath],
            elementDeclaredNames(parts.firstStage),
            depth + 1,
         );
         return;
      }
      for (const [k, child] of Object.entries(v)) {
         if (options.skipKeys.has(k)) continue;
         walk(child, prefix, declared, depth + 1);
      }
   };
   walk(e, [], new Set(), 0);
}

/** A join an array query's pipeline declares. */
export interface ArrayQueryJoin {
   /** The join's name in the elements' query. */
   alias: string;
   /** Where the join was written, when the compiler recorded it. */
   at?: { url?: string; line?: number };
}

/** One shared empty result, so a clean subtree allocates nothing. */
const NO_JOINS: ArrayQueryJoin[] = [];

/** Per object outside any array query: the joins under it, read once. */
const joinsMemo = new WeakMap<object, ArrayQueryJoin[]>();

function locationOf(f: IrObject): ArrayQueryJoin["at"] {
   const loc = isObject(f.location) ? f.location : undefined;
   if (!loc) return undefined;
   const range = isObject(loc.range) ? loc.range : undefined;
   const start = range && isObject(range.start) ? range.start : undefined;
   return {
      ...(typeof loc.url === "string" ? { url: loc.url } : {}),
      ...(typeof start?.line === "number" ? { line: start.line + 1 } : {}),
   };
}

/** Keys that hold no expression, source or pipeline. */
const NOT_IR = new Set(["at", "location", "annotations"]);

/** The source joins any `extend:` under one array query's node declares. */
function joinsInside(arrayQuery: IrObject): ArrayQueryJoin[] {
   const found: ArrayQueryJoin[] = [];
   const seen = new Set<object>();
   const walk = (v: unknown): void => {
      if (!isObject(v) || seen.has(v)) return;
      seen.add(v);
      if (Array.isArray(v.extendSource)) {
         for (const f of v.extendSource) {
            if (isObject(f) && f.join && isSourceDef(f as never)) {
               const alias = typeof f.as === "string" ? f.as : f.name;
               const at = locationOf(f);
               found.push({
                  alias: typeof alias === "string" ? alias : "",
                  ...(at ? { at } : {}),
               });
            }
         }
      }
      for (const [k, child] of Object.entries(v)) {
         if (!NOT_IR.has(k)) walk(child);
      }
   };
   walk(arrayQuery);
   return found;
}

/**
 * Every join to a source declared anywhere inside an array query's pipeline
 * (any stage, a nest in it, or an array query nested in it) under `root`:
 * a model definition, a source, or a compiled query. A join to a record or
 * array (an element's own nested array) is not one. Memoized per object, so
 * a source shared by many queries is read once.
 */
export function arrayQueryJoins(root: unknown): ArrayQueryJoin[] {
   const inProgress = new Set<object>();
   const walk = (v: unknown): ArrayQueryJoin[] => {
      if (!isObject(v)) return [];
      const memo = joinsMemo.get(v);
      if (memo) return memo;
      // The IR is a tree with shared nodes; a cycle reads nothing twice.
      if (inProgress.has(v)) return [];
      inProgress.add(v);
      let found: ArrayQueryJoin[] = NO_JOINS;
      if (v.node === "arrayQuery") {
         found = joinsInside(v);
      } else {
         for (const [k, child] of Object.entries(v)) {
            if (NOT_IR.has(k)) continue;
            const under = walk(child);
            if (under.length > 0) found = [...found, ...under];
         }
      }
      inProgress.delete(v);
      joinsMemo.set(v, found);
      return found;
   };
   return walk(root);
}

const governsMemo = new WeakMap<object, boolean>();

/**
 * Whether anything under `root` (a model definition, imports folded in)
 * carries an `#(authorize)`, `#(access_filter)`, `#(carries)` or
 * `#(classification)` note: a check of Publisher's reads what a query reads,
 * or which source it reaches, so a read it cannot follow matters here.
 */
export function governsAccess(root: unknown): boolean {
   if (!isObject(root)) return false;
   const memo = governsMemo.get(root);
   if (memo !== undefined) return memo;
   const seen = new Set<object>();
   const walk = (v: unknown): boolean => {
      if (!isObject(v) || seen.has(v)) return false;
      seen.add(v);
      if (Array.isArray(v)) return v.some(walk);
      for (const key of ["notes", "blockNotes"]) {
         const notes = v[key];
         if (!Array.isArray(notes)) continue;
         for (const note of notes) {
            const text = isObject(note) ? note.text : undefined;
            if (
               typeof text === "string" &&
               GOVERNING_ROUTES.some((route) => isOnRoute(text, route))
            ) {
               return true;
            }
         }
      }
      for (const [k, child] of Object.entries(v)) {
         if (k === "at" || k === "location") continue;
         if (walk(child)) return true;
      }
      return false;
   };
   const governs = walk(root);
   governsMemo.set(root, governs);
   return governs;
}

/**
 * The refusal for the joins {@link arrayQueryJoins} found where `where` (a
 * model, or "this query") governs access, or undefined when there are none.
 */
export function arrayQueryJoinRefusal(
   where: string,
   joins: readonly ArrayQueryJoin[],
): string | undefined {
   if (joins.length === 0) return undefined;
   // A source the model reaches by two paths holds one join, listed once.
   const named = Array.from(
      new Set(
         joins.map((j) => {
            const line = j.at?.line !== undefined ? ` (line ${j.at.line})` : "";
            return `\`${j.alias}\`${line}`;
         }),
      ),
   ).join(", ");
   return (
      `${where} joins a source inside a query on an array (\`items -> { extend: { join_one: … } }\`): ${named}. ` +
      `Publisher cannot follow what such a join reads, so #(authorize), #(access_filter), #(carries), ` +
      `#(classification) and the access record cannot account for it, and a model that declares any ` +
      `of them refuses it. Remove the join from the array query.`
   );
}
