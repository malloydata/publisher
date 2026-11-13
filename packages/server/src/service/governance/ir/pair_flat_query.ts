// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How an aggregate reads a pair table served flat (materialization v2, P5):
 * the ONE reader of compiled measure and query IR for it. Malloy exports no
 * type for any of this, so every node is duck-typed here, once.
 *
 * A pair table stores one row per row of live's LEFT JOIN through an inlined
 * join_many, and the served copy reads it as the source itself: the join's
 * name is a record of the child's columns on the same row, and one flag marks
 * one row per parent. On those rows:
 *
 *  - an aggregate through the join (`j.x.sum()`, `j.count()`) reads every
 *    row, which are the rows live aggregates through the join, so it is exact
 *    as it stands, live's count of a childless parent (malloydata/malloy#1762)
 *    included;
 *  - an aggregate over the source's own fields is exact only under the
 *    parent-row flag (one row per parent), and only where nothing it is
 *    grouped or filtered by reads the child: a parent's flagged row is one of
 *    its rows, so a child value would place the parent in that row's group
 *    alone, where live places it in every group one of its children is in.
 *
 * {@link pairMeasureLevels} says which measures the served copy re-declares
 * under the flag; {@link flatPairQueryRefusal} says whether one compiled query
 * is exact over the flat table, and why not. Anything it cannot read is a
 * refusal: the query is then answered live.
 *
 * Exact is not enough: a query that reads nothing through the join (only the
 * source's own fields) reads each parent off its flagged row, which costs a
 * scan of every pair row where live scans the parents alone (4 to 5.5 times
 * live on the cheapest such query). {@link flatPairQueryVerdict} sends it to
 * the next candidate ({@link pairQueryReadsChild} decides, and fails closed:
 * a query it cannot read is not answered from the pair table either).
 */

/** The pair table a query is checked against. */
export interface FlatPair {
   /** The inlined join_many's name: on the served source, the record. */
   join: string;
   /** The parent-row flag's (internal) field on the served source. */
   parentRowField: string;
   /**
    * The joined source's fields that carry annotations, which the record
    * cannot: a query that outputs one is answered live, with them.
    */
   annotatedChildFields: ReadonlySet<string>;
}

/** Where an aggregate (or a value it reads) lives. */
export type PairLevel = "parent" | "child";

interface Node {
   node?: unknown;
   path?: unknown;
   function?: unknown;
   structPath?: unknown;
   e?: unknown;
   kids?: unknown;
   overload?: { returnType?: { expressionType?: unknown } };
   filterList?: unknown;
}

interface Field {
   name?: unknown;
   as?: unknown;
   type?: unknown;
   join?: unknown;
   e?: unknown;
   expressionType?: unknown;
   pipeline?: unknown;
}

interface Segment {
   type?: unknown;
   queryFields?: unknown;
   filterList?: unknown;
   extendSource?: unknown;
   expandedUngroupings?: unknown;
}

/** Thrown inside a walk: the reason a query or measure is not exact flat. */
class Refusal extends Error {}

const MAX_DEPTH = 64;

/** A source's fields by the name a query reads them under (`as`, then `name`). */
function fieldsByName(fields: unknown): Map<string, Field> {
   const out = new Map<string, Field>();
   for (const f of Array.isArray(fields) ? (fields as Field[]) : []) {
      const n = typeof f.as === "string" ? f.as : f.name;
      if (typeof n === "string") out.set(n, f);
   }
   return out;
}

function pathOf(n: Node): string[] | undefined {
   return Array.isArray(n.path) && n.path.every((p) => typeof p === "string")
      ? (n.path as string[])
      : undefined;
}

function children(n: Node): Node[] {
   const out: Node[] = [];
   const add = (v: unknown) => {
      if (Array.isArray(v)) {
         for (const x of v) add(x);
      } else if (v && typeof v === "object") {
         out.push(v as Node);
      }
   };
   if (n.kids && typeof n.kids === "object") {
      for (const v of Object.values(n.kids as object)) add(v);
   }
   if (n.e && typeof n.e === "object") add(n.e);
   return out;
}

/** What walking one expression found. */
interface Reads {
   parent: boolean;
   child: boolean;
}

/**
 * The walker over one source's fields: `join` is the inlined join_many (a
 * join in the author's compile, a record in the served copy).
 */
class Walker {
   constructor(
      private readonly fields: Map<string, Field>,
      private readonly join: string,
      private readonly parentRowField: string | undefined,
   ) {}

   /**
    * Refuse a path through a join other than the inlined one that names an
    * aggregate or a view of the joined source.
    */
   throughJoin(path: string[]): void {
      let fields = this.fields;
      for (let i = 0; i < path.length; i++) {
         const f = fields.get(path[i]);
         if (!f)
            throw new Refusal(
               `the query reads \`${path.join(".")}\`, unresolved`,
            );
         if (i === path.length - 1) {
            if (isAggregateField(f) || f.type === "turtle") {
               throw new Refusal(
                  `the query reads \`${path.join(".")}\`, an aggregate through another join than the inlined one`,
               );
            }
            return;
         }
         fields = fieldsByName((f as { fields?: unknown }).fields);
      }
   }

   /** Which side a field path reads, resolving dimensions through definitions. */
   readsOfPath(path: string[], depth: number): Reads {
      if (depth > MAX_DEPTH) throw new Refusal("an expression nests too deep");
      if (path[0] === this.join) return { parent: false, child: true };
      const f = this.fields.get(path[0]);
      if (!f) throw new Refusal(`the query reads \`${path[0]}\`, unresolved`);
      if (path.length > 1) {
         this.throughJoin(path);
         if (f.join === "one" || f.type === "record") {
            return { parent: true, child: false };
         }
         throw new Refusal(
            `the query reads \`${path[0]}\`, a join that fans out a pair table's rows`,
         );
      }
      if (f.e && typeof f.e === "object") {
         return this.readsOf(f.e as Node, depth + 1);
      }
      return { parent: true, child: false };
   }

   /** Which sides a scalar expression reads (no aggregate may be in it). */
   readsOf(n: Node, depth: number): Reads {
      if (depth > MAX_DEPTH) throw new Refusal("an expression nests too deep");
      const out: Reads = { parent: false, child: false };
      if (n.node === "aggregate") {
         throw new Refusal("an aggregate where a value was expected");
      }
      if (n.node === "field") {
         const p = pathOf(n);
         if (!p) throw new Refusal("a field reference that cannot be read");
         return this.readsOfPath(p, depth + 1);
      }
      for (const k of children(n)) {
         const r = this.readsOf(k, depth + 1);
         out.parent ||= r.parent;
         out.child ||= r.child;
      }
      return out;
   }

   /**
    * Every aggregate an aggregate-valued expression reaches, through the
    * measures it reads by name, with whether it sits under the parent-row
    * flag and whether a filter on it reads the child.
    */
   aggregates(
      n: Node,
      ctx: { flagged: boolean; childFilter: boolean; inAggregate: boolean },
      visit: (
         level: PairLevel,
         ctx: { flagged: boolean; childFilter: boolean },
      ) => void,
      depth: number,
   ): void {
      if (depth > MAX_DEPTH) throw new Refusal("an expression nests too deep");
      switch (n.node) {
         case "all":
         case "exclude":
            throw new Refusal(
               "an aggregate ungrouped with all() or exclude(), which a pair table cannot regroup",
            );
         case "aggregate": {
            if (ctx.inAggregate)
               throw new Refusal("an aggregate inside another");
            const sp = Array.isArray(n.structPath)
               ? (n.structPath as unknown[])
               : [];
            const arg = (n.e ?? {}) as Node;
            const reads = this.readsOf(arg, depth + 1);
            let level: PairLevel;
            if (sp.length > 0) {
               if (sp[0] !== this.join) {
                  throw new Refusal(
                     `an aggregate through \`${String(sp[0])}\`, another join than the inlined one`,
                  );
               }
               level = "child";
            } else if (reads.child && reads.parent) {
               throw new Refusal(
                  "an aggregate that reads both the source's own fields and the inlined join's",
               );
            } else {
               // Malloy places no aggregate that ignores duplicates (`max`,
               // `min`, a distinct count) at a join: one over the join's
               // fields alone reads every row, as live's does.
               level = reads.child ? "child" : "parent";
            }
            visit(level, ctx);
            return;
         }
         case "filteredExpr": {
            const kids = (n.kids ?? {}) as { e?: Node; filterList?: unknown };
            let flagged = ctx.flagged;
            let childFilter = ctx.childFilter;
            for (const c of Array.isArray(kids.filterList)
               ? (kids.filterList as Node[])
               : []) {
               const e = (c.e ?? {}) as Node;
               const p = e.node === "field" ? pathOf(e) : undefined;
               if (
                  this.parentRowField !== undefined &&
                  p?.length === 1 &&
                  p[0] === this.parentRowField
               ) {
                  flagged = true;
                  continue;
               }
               if (this.readsOf(e, depth + 1).child) childFilter = true;
            }
            if (!kids.e)
               throw new Refusal("a filtered aggregate with no value");
            this.aggregates(
               kids.e,
               { ...ctx, flagged, childFilter },
               visit,
               depth + 1,
            );
            return;
         }
         case "field": {
            const p = pathOf(n);
            if (!p) throw new Refusal("a field reference that cannot be read");
            if (p.length === 1) {
               const f = this.fields.get(p[0]);
               if (f && isAggregateField(f) && f.e && typeof f.e === "object") {
                  this.aggregates(f.e as Node, ctx, visit, depth + 1);
               }
               return;
            }
            // A record field is a value; a measure through another join is
            // an aggregate this walk cannot place.
            if (p[0] !== this.join) this.throughJoin(p);
            return;
         }
         case "function_call": {
            const t = n.overload?.returnType?.expressionType;
            if (typeof t === "string" && t !== "scalar") {
               throw new Refusal(
                  `a ${t.replace("_", " ")} function, whose value a pair table does not reproduce`,
               );
            }
            break;
         }
      }
      for (const k of children(n)) this.aggregates(k, ctx, visit, depth + 1);
   }
}

function isAggregateField(f: Field): boolean {
   return (
      f.expressionType === "aggregate" ||
      f.expressionType === "ungrouped_aggregate"
   );
}

/**
 * For each measure of a source (its compiled fields; `join` is the inlined
 * join_many), the levels its aggregates read, through the measures it reads
 * by name; a measure that cannot be read at all (an ungrouped or windowed
 * aggregate, one through another join, one reading both sides) is absent.
 */
export function pairMeasureLevels(
   sourceFields: unknown,
   join: string,
): Map<string, Set<PairLevel>> {
   const fields = fieldsByName(sourceFields);
   const walker = new Walker(fields, join, undefined);
   const out = new Map<string, Set<PairLevel>>();
   for (const [name, f] of fields) {
      if (!isAggregateField(f) || !f.e || typeof f.e !== "object") continue;
      const levels = new Set<PairLevel>();
      try {
         walker.aggregates(
            f.e as Node,
            { flagged: false, childFilter: false, inAggregate: false },
            (level, ctx) => {
               levels.add(level);
               // A filter reading the child on a parent-level aggregate keeps
               // the parent on its flagged row alone.
               if (level === "parent" && ctx.childFilter) {
                  throw new Refusal("a parent aggregate filtered by the child");
               }
            },
            0,
         );
      } catch (e) {
         if (e instanceof Refusal) continue;
         throw e;
      }
      out.set(name, levels);
   }
   return out;
}

/**
 * Why a compiled query is not exact over a pair table served flat, in the
 * caller's own terms, or undefined when it is. `query` is the prepared
 * query's `_query`, `source` its run target's struct (the served source, or a
 * source that extends it).
 */
export function flatPairQueryRefusal(
   query: { pipeline?: unknown } | undefined,
   source: { fields?: unknown; filterList?: unknown } | undefined,
   pair: FlatPair,
   /**
    * A filter applied to the source outside the query reads the child (a
    * row-level gate grafted at read, {@link pairConditionReadsChild}): parent
    * aggregates are then refused as under a source `where:` on the child.
    */
   childScoped = false,
): string | undefined {
   try {
      const fields = fieldsByName(source?.fields);
      const record = fields.get(pair.join);
      if (!record || record.type !== "record") {
         throw new Refusal(
            `the queried source does not read \`${pair.join}\` from its pair table`,
         );
      }
      const pipeline = Array.isArray(query?.pipeline)
         ? (query?.pipeline as Segment[])
         : [];
      if (pipeline.length === 0) throw new Refusal("a query with no stage");
      const walker = new Walker(fields, pair.join, pair.parentRowField);
      const sourceFilter =
         childScoped || filtersReadChild(walker, source?.filterList, 0);
      checkSegment(walker, fields, pipeline[0], pair, sourceFilter, 0);
      return undefined;
   } catch (e) {
      if (e instanceof Refusal) return e.message;
      throw e;
   }
}

function filtersReadChild(
   walker: Walker,
   filterList: unknown,
   depth: number,
): boolean {
   let child = false;
   for (const c of Array.isArray(filterList) ? (filterList as Node[]) : []) {
      const f = c as Node & { expressionType?: unknown };
      if (f.expressionType !== undefined && f.expressionType !== "scalar") {
         continue;
      }
      if (walker.readsOf((c.e ?? {}) as Node, depth + 1).child) child = true;
   }
   return child;
}

/**
 * One stage of a query (or of a nest in it): every value it groups by, every
 * filter, then every aggregate, under what its enclosing stages group and
 * filter by.
 */
function checkSegment(
   outer: Walker,
   sourceFields: Map<string, Field>,
   segment: Segment,
   pair: FlatPair,
   childScoped: boolean,
   depth: number,
): void {
   if (depth > MAX_DEPTH) throw new Refusal("a query nests too deep");
   if (segment.type !== "reduce") {
      throw new Refusal(
         segment.type === "project"
            ? "a select: over a pair table, which stores a parent once per child"
            : `a ${String(segment.type)} stage over a pair table`,
      );
   }
   if (
      Array.isArray(segment.expandedUngroupings) &&
      segment.expandedUngroupings.length > 0
   ) {
      throw new Refusal(
         "an aggregate ungrouped with all() or exclude(), which a pair table cannot regroup",
      );
   }
   // A stage's own `extend:` fields resolve alongside the source's.
   const local = new Map(sourceFields);
   const ext = (segment.extendSource ?? []) as Field[];
   for (const [n, f] of fieldsByName(ext)) local.set(n, f);
   const walker =
      local.size === sourceFields.size
         ? outer
         : new Walker(local, pair.join, pair.parentRowField);
   const queryFields = Array.isArray(segment.queryFields)
      ? (segment.queryFields as (Field & { path?: unknown })[])
      : [];
   let childGrouped = childScoped;
   const aggregates: Node[] = [];
   const nests: Segment[][] = [];
   for (const qf of queryFields) {
      if (qf.type === "fieldref") {
         const p = pathOf(qf as Node);
         if (!p) throw new Refusal("a field reference that cannot be read");
         if (p[0] === pair.join) {
            if (p.length === 2 && pair.annotatedChildFields.has(p[1])) {
               throw new Refusal(
                  `the query outputs \`${p.join(".")}\`, whose annotations its pair table's record cannot carry`,
               );
            }
            childGrouped = true;
            continue;
         }
         if (p.length > 1) walker.throughJoin(p);
         const f = p.length === 1 ? local.get(p[0]) : undefined;
         if (f && f.type === "turtle") {
            nests.push(pipelineOf(f));
            continue;
         }
         if (f && isAggregateField(f)) {
            aggregates.push({ node: "field", path: p });
            continue;
         }
         if (
            f &&
            typeof f.expressionType === "string" &&
            f.expressionType.includes("analytic")
         ) {
            throw new Refusal(
               "a window function, whose value a pair table does not reproduce",
            );
         }
         if (walker.readsOfPath(p, depth + 1).child) childGrouped = true;
         continue;
      }
      if (qf.type === "turtle") {
         nests.push(pipelineOf(qf));
         continue;
      }
      const t = qf.expressionType;
      if (typeof t === "string" && t.includes("analytic")) {
         throw new Refusal(
            "a window function, whose value a pair table does not reproduce",
         );
      }
      if (!qf.e || typeof qf.e !== "object") {
         throw new Refusal("a query field that cannot be read");
      }
      if (t === "aggregate" || t === "ungrouped_aggregate") {
         aggregates.push(qf.e as Node);
         continue;
      }
      if (walker.readsOf(qf.e as Node, depth + 1).child) childGrouped = true;
   }
   let childFiltered = childGrouped;
   for (const c of Array.isArray(segment.filterList)
      ? (segment.filterList as (Node & { expressionType?: unknown })[])
      : []) {
      if (c.expressionType === "aggregate") {
         aggregates.push((c.e ?? {}) as Node);
      } else if (walker.readsOf((c.e ?? {}) as Node, depth + 1).child) {
         childFiltered = true;
      }
   }
   for (const a of aggregates) {
      walker.aggregates(
         a,
         { flagged: false, childFilter: false, inAggregate: false },
         (level, ctx) => {
            if (level === "child") {
               if (ctx.flagged) {
                  throw new Refusal(
                     "an aggregate through the inlined join under the parent-row filter",
                  );
               }
               return;
            }
            if (!ctx.flagged) {
               throw new Refusal(
                  "an aggregate over the source's own fields that is not one of its measures, which a pair table stores once per child",
               );
            }
            if (childFiltered || ctx.childFilter) {
               throw new Refusal(
                  "an aggregate over the source's own fields, grouped or filtered by the inlined join's fields",
               );
            }
         },
         depth + 1,
      );
   }
   for (const pipeline of nests) {
      if (pipeline.length === 0) throw new Refusal("a nest with no stage");
      checkSegment(walker, local, pipeline[0], pair, childFiltered, depth + 1);
   }
}

function pipelineOf(f: Field): Segment[] {
   return Array.isArray(f.pipeline) ? (f.pipeline as Segment[]) : [];
}

/**
 * Whether a compiled query reads anything through the inlined join: a field,
 * an aggregate, a filter, a group-by or a nest through it, directly or through
 * the definitions it reads by name (a dimension, a measure, a view), in its
 * first stage, a nest's, or the source's own `where:`. Undefined when the query
 * cannot be read (a reference that does not resolve, a node of no known
 * shape): the caller then treats it as not answerable from the pair table.
 * `query` and `source` are as {@link flatPairQueryRefusal} takes them.
 */
export function pairQueryReadsChild(
   query: { pipeline?: unknown } | undefined,
   source: { fields?: unknown; filterList?: unknown } | undefined,
   join: string,
): boolean | undefined {
   try {
      const fields = fieldsByName(source?.fields);
      const pipeline = Array.isArray(query?.pipeline)
         ? (query?.pipeline as Segment[])
         : [];
      if (pipeline.length === 0) return undefined;
      const scan = new ChildScan(join);
      for (const c of Array.isArray(source?.filterList)
         ? (source?.filterList as unknown[])
         : []) {
         if (scan.value(c, fields, 0)) return true;
      }
      return scan.stage(pipeline[0], fields, 0);
   } catch (e) {
      if (e instanceof Refusal) return undefined;
      throw e;
   }
}

/**
 * Whether a compiled filter condition (a row-level gate's, lifted from its
 * probe) on `source` reads anything through the inlined join. Undefined when
 * it cannot be read; the caller then counts it as reading the child.
 */
export function pairConditionReadsChild(
   condition: unknown,
   source: { fields?: unknown } | undefined,
   join: string,
): boolean | undefined {
   try {
      return new ChildScan(join).value(
         condition,
         fieldsByName(source?.fields),
         0,
      );
   } catch (e) {
      if (e instanceof Refusal) return undefined;
      throw e;
   }
}

/**
 * The walk behind {@link pairQueryReadsChild}: every node of a stage, each
 * field reference resolved by name through its definition, each nest by its
 * first stage. Later stages read the first stage's output, not the source.
 */
class ChildScan {
   /** Each definition's answer, read once however often it is named. */
   private readonly resolved = new WeakMap<object, boolean>();
   /** The joins whose condition is being read, which read their own fields. */
   private readonly joining = new Set<string>();

   constructor(private readonly join: string) {}

   stage(segment: unknown, fields: Map<string, Field>, depth: number): boolean {
      if (depth > MAX_DEPTH) throw new Refusal("a query nests too deep");
      if (!segment || typeof segment !== "object") {
         throw new Refusal("a stage that cannot be read");
      }
      const s = segment as Segment;
      const local = new Map(fields);
      for (const [n, f] of fieldsByName(s.extendSource)) local.set(n, f);
      for (const [k, v] of Object.entries(s)) {
         // A stage's own `extend:` is read where the stage names it; its
         // output schema is what it produces, not what it reads.
         if (k === "extendSource" || k === "outputStruct") continue;
         if (this.value(v, local, depth + 1)) return true;
      }
      return false;
   }

   value(v: unknown, fields: Map<string, Field>, depth: number): boolean {
      if (depth > MAX_DEPTH) throw new Refusal("an expression nests too deep");
      if (Array.isArray(v)) {
         return v.some((x) => this.value(x, fields, depth + 1));
      }
      if (!v || typeof v !== "object") return false;
      const n = v as Node & Field & { type?: unknown };
      if (Array.isArray(n.structPath) && n.structPath[0] === this.join) {
         return true;
      }
      if (n.node === "field" || n.type === "fieldref") {
         const p = pathOf(n);
         if (!p) throw new Refusal("a field reference that cannot be read");
         return this.path(p, fields, depth + 1);
      }
      if (n.type === "turtle" && n.pipeline !== undefined) {
         const nest = pipelineOf(n);
         if (nest.length === 0) throw new Refusal("a nest with no stage");
         return this.stage(nest[0], fields, depth + 1);
      }
      return Object.values(v).some((x) => this.value(x, fields, depth + 1));
   }

   private path(
      p: string[],
      fields: Map<string, Field>,
      depth: number,
   ): boolean {
      if (p[0] === this.join) return true;
      const f = fields.get(p[0]);
      if (!f) throw new Refusal(`the query reads \`${p[0]}\`, unresolved`);
      if (p.length > 1) {
         // Through another join: the joined source's own field, reached by
         // that join's condition, which may read the inlined join (its reads
         // of the join's own fields are the join's, not the parent's).
         if (this.joining.has(p[0])) return false;
         const on = (f as { onExpression?: unknown }).onExpression;
         if (on === undefined) return false;
         this.joining.add(p[0]);
         try {
            return this.value(on, fields, depth + 1);
         } finally {
            this.joining.delete(p[0]);
         }
      }
      if (f.type === "turtle") {
         const nest = pipelineOf(f);
         if (nest.length === 0) throw new Refusal("a view with no stage");
         return this.stage(nest[0], fields, depth + 1);
      }
      if (f.e === undefined) return false;
      const known = this.resolved.get(f);
      if (known !== undefined) return known;
      const reads = this.value(f.e, fields, depth + 1);
      this.resolved.set(f, reads);
      return reads;
   }
}

/** What {@link flatPairQueryVerdict} decides for one query over a pair table. */
export type FlatPairVerdict =
   /** Answered from the pair table, exactly. */
   | { kind: "served" }
   /**
    * Reads nothing through the join: exact or not, the pair table is not the
    * table to read it from; the next candidate answers.
    */
   | { kind: "parent_only" }
   /** Not exact over the pair table, or not readable; `reason` says why. */
   | { kind: "refused"; reason: string };

/**
 * Whether the pair table answers a compiled query: only one that reads the
 * inlined join (anything else reads each parent off a flag over every pair
 * row, slower than live) and that the flat rows reproduce exactly.
 */
export function flatPairQueryVerdict(
   query: { pipeline?: unknown } | undefined,
   source: { fields?: unknown; filterList?: unknown } | undefined,
   pair: FlatPair,
   /** As {@link flatPairQueryRefusal} takes it. */
   childScoped = false,
): FlatPairVerdict {
   const child = pairQueryReadsChild(query, source, pair.join);
   if (child === undefined) {
      return {
         kind: "refused",
         reason:
            "the query could not be read to tell whether it reads the inlined join",
      };
   }
   if (!child) return { kind: "parent_only" };
   const reason = flatPairQueryRefusal(query, source, pair, childScoped);
   return reason === undefined
      ? { kind: "served" }
      : { kind: "refused", reason };
}
