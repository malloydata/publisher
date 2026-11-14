// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How an aggregate reads a pair table served flat (materialization v2, P5):
 * the ONE reader of compiled measure and query IR for it. Malloy exports no
 * type for any of this, so every node is duck-typed here, once.
 *
 * A pair table stores one row per row of live's LEFT JOIN through an inlined
 * path of join_manys (one hop, `segments`, or several, `items.options`), and
 * the served copy reads it as the source itself: the path's first join is a
 * record of the stored columns on the same row (a nested record per further
 * hop), and one flag per level above the last marks one row per distinct
 * row of that level (`__parent_row`: one per parent). Levels count from the
 * source (0) to the last hop (the leaf, `depth`). On those rows:
 *
 *  - an aggregate at the leaf (`j.x.sum()`, `items.options.x.sum()`, a `max`)
 *    reads every row, the null-extended row of a parent with no child
 *    included. Live reads that row for a `min` or `max`, and, since
 *    malloydata/malloy#3208 (Malloy 0.0.438), skips it for a `count()`,
 *    `sum` or `avg`, which test the join's key. So a sum or average at the
 *    leaf is exact as it stands only of a stored column of the leaf, null on
 *    that row; a count, or a sum or average of anything else, is exact only
 *    under the level's presence field ({@link FlatPair.levelPresentFields}),
 *    which skips that row as live does ({@link unmatchedRowRefusal});
 *  - an aggregate at a level above the leaf is exact only under that level's
 *    flag, and only where nothing it is grouped or filtered by reads a deeper
 *    level: a flagged row is one of its level's rows, so a deeper value would
 *    place it in that row's group alone, where live places it in every group
 *    one of its rows is in;
 *  - an aggregate at a level between the source and the leaf is exact when
 *    the query reads nothing deeper: live then joins down to that level and
 *    aggregates its rows plainly, which the flag marks (a childless row's
 *    null-extended row included, so a count there, or a sum or average of a
 *    value not null on that row, needs the level's presence field too, as at
 *    the leaf). Once the query reads
 *    deeper live aggregates that level symmetrically, keyed by its primary key
 *    (a childless parent then counts as no row): the flag reproduces that only
 *    where each row of the level has one parent row
 *    ({@link FlatPair.oneParentLevels}), for a sum or average of a stored
 *    column, or under the level's presence field.
 *
 * {@link pairMeasureLevels} says which measures the served copy re-declares
 * under the flag, and {@link pairMeasurePresence} which under the level's
 * presence field; {@link flatPairQueryRefusal} says whether one compiled query
 * is exact over the flat table, and why not. Anything it cannot read is a
 * refusal: the query is then answered live.
 *
 * Exact is not enough: a query that reads nothing through the join (only the
 * source's own fields) reads each parent off its flagged row, which costs a
 * scan of every pair row where live scans the parents alone (4 to 5.5 times
 * live on the cheapest such query). {@link flatPairQueryVerdict} sends it to
 * the next candidate ({@link pairQueryReadLevel} decides, and fails closed:
 * a query it cannot read is not answered from the pair table either).
 */

/** The pair table a query is checked against. */
export interface FlatPair {
   /** The inlined path's first join_many: on the served source, the record. */
   join: string;
   /** The parent-row flag's (internal) field on the served source. */
   parentRowField: string;
   /**
    * For a path of more than one hop: the joins past the first, in order,
    * each a record nested in the one before (`["options"]` for
    * `items.options`). Absent: one hop.
    */
   hops?: readonly string[];
   /**
    * For a path of more than one hop: the row flag of each level between the
    * source and the leaf (level 1 first), its (internal) field on the served
    * source.
    */
   levelRowFields?: readonly string[];
   /**
    * For a table that stores more than one path (one block each): the other
    * blocks' records on the served source, which read nulls on this block's
    * rows, so a query that reads one is never answered from this block.
    */
   otherRecords?: readonly string[];
   /**
    * For a path of more than one hop: the levels between the source and the
    * leaf each of whose rows has exactly one row of every level above it
    * (each join down to it keyed on its parent level's primary key, which the
    * build checks unique and never null). Under that level's flag such a row
    * is flagged once whichever parent groups it, so a sum or average of one
    * of its stored columns there equals live's symmetric aggregate by its key
    * beside deeper fields.
    */
   oneParentLevels?: readonly number[];
   /**
    * The path's computed fields the pair table stores (`items.flag`), whose
    * value on a childless row's null-extended row need not be null.
    */
   computedFields?: readonly string[];
   /**
    * For each level of the path (level 1 first, the leaf last): the
    * (internal) field on the served source that is true on a row where that
    * level has a row (its primary key is not null), or undefined where the
    * served copy declares none. Since malloydata/malloy#3208 live takes a
    * `count()`, `sum` or `avg` at a join over the rows where the join's key
    * is not null; under this field an aggregate of the flat rows does the
    * same. The served copy declares it only where the build checked that
    * level's key unique and never null (a level above the leaf on every
    * build, the leaf where its recorded check passed), so the field is false
    * on exactly the null-extended row of a row with nothing at that level.
    */
   levelPresentFields?: readonly (string | undefined)[];
   /**
    * Whether the leaf's key is known unique and never null on a row of the
    * leaf (its check, recorded beside the table, passed). Without it no
    * `count()`, `sum` or `avg` at the leaf is exact, not even of a stored
    * column: live skips a child whose key is NULL (malloydata/malloy#3208),
    * which the pair table holds as a row like any other, and the build's
    * distinct rows store two equal children of one key as one row. A level
    * above the leaf is checked on every build, which fails without it.
    */
   leafKeyChecked?: boolean;
}

/**
 * Where an aggregate (or a value it reads) lives: 0 for the source's own
 * fields, 1 for the path's first join, and so on to the leaf.
 */
type PairLevel = number;

/** The number of levels below the source: the leaf's level. */
function depthOf(pair: Pick<FlatPair, "hops">): number {
   return 1 + (pair.hops?.length ?? 0);
}

/**
 * The level an aggregate's `structPath` aggregates at along `pair`'s path
 * (`items` is level 1 however deep the path goes), or 0 when off it.
 */
function levelOfStruct(
   names: readonly unknown[],
   pair: Pick<FlatPair, "join" | "hops">,
): number {
   if (names[0] !== pair.join) return 0;
   const hops = pair.hops ?? [];
   let level = 1;
   while (level - 1 < hops.length && names[level] === hops[level - 1]) {
      level++;
   }
   return level;
}

/** The level a path of names reads along `pair`'s path, or 0 when off it. */
function levelOfNames(
   names: readonly unknown[],
   pair: Pick<FlatPair, "join" | "hops">,
): number {
   if (names[0] !== pair.join) return 0;
   const hops = pair.hops ?? [];
   let level = 1;
   while (level - 1 < hops.length && names[level] === hops[level - 1]) {
      level++;
   }
   // A path that ends at a record (`items`, or `items.options` above a
   // deeper hop) reads that record whole, every level below it included.
   return names.length === level ? depthOf(pair) : level;
}

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

/** The levels one expression reads: 0 for the source's own fields. */
type Reads = Set<number>;

/** The deepest level in `reads`, or 0. */
function deepest(reads: Iterable<number>): number {
   let out = 0;
   for (const l of reads) if (l > out) out = l;
   return out;
}

/** What an aggregate's context says: the flag it sits under, and its filters. */
interface AggregateContext {
   /** The level whose row flag filters it, when one does. */
   flagged: number | undefined;
   /** The deepest level its other filters read (0: none below the source). */
   filterLevel: number;
   /** The levels whose presence field filters it. */
   present?: readonly number[];
   inAggregate: boolean;
}

/**
 * The walker over one source's fields: `pair` names the inlined path (a join
 * in the author's compile, a record in the served copy).
 */
class Walker {
   private readonly flags: Map<string, number>;
   private readonly present: Map<string, number>;

   constructor(
      private readonly fields: Map<string, Field>,
      private readonly pair: Pick<
         FlatPair,
         "join" | "hops" | "otherRecords"
      > & {
         parentRowField?: string;
         levelRowFields?: readonly string[];
         levelPresentFields?: readonly (string | undefined)[];
      },
   ) {
      this.present = new Map();
      (pair.levelPresentFields ?? []).forEach((f, i) => {
         if (f !== undefined) this.present.set(f, i + 1);
      });
      this.flags = new Map();
      if (pair.parentRowField !== undefined) {
         this.flags.set(pair.parentRowField, 0);
      }
      (pair.levelRowFields ?? []).forEach((f, i) => this.flags.set(f, i + 1));
   }

   get join(): string {
      return this.pair.join;
   }

   /** The leaf's level. */
   get depth(): number {
      return depthOf(this.pair);
   }

   /** The level of the row flag a field names, when it names one. */
   flagLevel(path: readonly string[] | undefined): number | undefined {
      return path?.length === 1 ? this.flags.get(path[0]) : undefined;
   }

   /** The level whose presence field a field names, when it names one. */
   presentLevel(path: readonly string[] | undefined): number | undefined {
      return path?.length === 1 ? this.present.get(path[0]) : undefined;
   }

   /**
    * Refuse a path through a join other than the inlined one that names an
    * aggregate or a view of the joined source, or that steps through a
    * join_many or cross join past its first join (a join_one's own join_many
    * fans out the pair rows as much as one on the source).
    */
   throughJoin(path: string[]): void {
      let fields = this.fields;
      for (let i = 0; i < path.length; i++) {
         const f = fields.get(path[i]);
         if (!f)
            throw new Refusal(
               `the query reads \`${path.join(".")}\`, unresolved`,
            );
         if (
            i > 0 &&
            i < path.length - 1 &&
            (f.join === "many" || f.join === "cross")
         ) {
            throw new Refusal(
               `the query reads \`${path.join(".")}\`, through a join that fans out a pair table's rows`,
            );
         }
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

   /** Which levels a field path reads, resolving dimensions through definitions. */
   readsOfPath(path: string[], depth: number): Reads {
      if (depth > MAX_DEPTH) throw new Refusal("an expression nests too deep");
      if (path[0] === this.join)
         return new Set([levelOfNames(path, this.pair)]);
      if (this.pair.otherRecords?.includes(path[0])) {
         throw new Refusal(
            `the query reads \`${path[0]}\`, another inlined join_many, which this pair table's rows do not hold`,
         );
      }
      const f = this.fields.get(path[0]);
      if (!f) throw new Refusal(`the query reads \`${path[0]}\`, unresolved`);
      if (path.length > 1) {
         this.throughJoin(path);
         if (f.join === "one" || f.type === "record") {
            return new Set([0]);
         }
         throw new Refusal(
            `the query reads \`${path[0]}\`, a join that fans out a pair table's rows`,
         );
      }
      if (f.e && typeof f.e === "object") {
         return this.readsOf(f.e as Node, depth + 1);
      }
      return new Set([0]);
   }

   /** Which levels a scalar expression reads (no aggregate may be in it). */
   readsOf(n: Node, depth: number): Reads {
      if (depth > MAX_DEPTH) throw new Refusal("an expression nests too deep");
      const out: Reads = new Set();
      if (n.node === "aggregate") {
         throw new Refusal("an aggregate where a value was expected");
      }
      if (n.node === "field") {
         const p = pathOf(n);
         if (!p) throw new Refusal("a field reference that cannot be read");
         return this.readsOfPath(p, depth + 1);
      }
      for (const k of children(n)) {
         for (const l of this.readsOf(k, depth + 1)) out.add(l);
      }
      return out;
   }

   /**
    * Every aggregate an aggregate-valued expression reaches, through the
    * measures it reads by name, with the level it aggregates at, the flag it
    * sits under and how deep a filter on it reads.
    */
   aggregates(
      n: Node,
      ctx: AggregateContext,
      visit: (level: PairLevel, ctx: AggregateContext, aggregate: Node) => void,
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
               if (
                  typeof sp[0] === "string" &&
                  this.pair.otherRecords?.includes(sp[0])
               ) {
                  throw new Refusal(
                     `an aggregate through \`${sp[0]}\`, another inlined join_many, which this pair table's rows do not hold`,
                  );
               }
               if (sp[0] !== this.join) {
                  throw new Refusal(
                     `an aggregate through \`${String(sp[0])}\`, another join than the inlined one`,
                  );
               }
               level = levelOfStruct(sp, this.pair);
               if (sp.length > level) {
                  throw new Refusal(
                     `an aggregate through \`${sp.map(String).join(".")}\`, past the inlined path`,
                  );
               }
               if (deepest(reads) > level) {
                  throw new Refusal(
                     "an aggregate over a level below the one it aggregates at",
                  );
               }
            } else if (reads.has(0) && deepest(reads) > 0) {
               throw new Refusal(
                  "an aggregate that reads both the source's own fields and the inlined join's",
               );
            } else {
               // Malloy places no aggregate that ignores duplicates (`max`,
               // `min`, a distinct count) at a join: one over the join's
               // fields alone is placed at the deepest level it reads, which
               // reads every row at the leaf, as live's does.
               const below = [...reads].filter((l) => l > 0);
               if (new Set(below).size > 1) {
                  throw new Refusal(
                     "an aggregate that reads more than one level of the inlined path",
                  );
               }
               level = deepest(reads);
            }
            visit(level, ctx, n);
            return;
         }
         case "filteredExpr": {
            const kids = (n.kids ?? {}) as { e?: Node; filterList?: unknown };
            let flagged = ctx.flagged;
            let filterLevel = ctx.filterLevel;
            let present = ctx.present ?? [];
            for (const c of Array.isArray(kids.filterList)
               ? (kids.filterList as Node[])
               : []) {
               const e = (c.e ?? {}) as Node;
               const named = e.node === "field" ? pathOf(e) : undefined;
               const presentAt = this.presentLevel(named);
               if (presentAt !== undefined) {
                  present = [...present, presentAt];
                  continue;
               }
               const flag = this.flagLevel(named);
               if (flag !== undefined) {
                  if (flagged !== undefined && flagged !== flag) {
                     throw new Refusal(
                        "an aggregate under the row flags of two levels",
                     );
                  }
                  flagged = flag;
                  continue;
               }
               filterLevel = Math.max(
                  filterLevel,
                  deepest(this.readsOf(e, depth + 1)),
               );
            }
            if (!kids.e)
               throw new Refusal("a filtered aggregate with no value");
            this.aggregates(
               kids.e,
               { ...ctx, flagged, filterLevel, present },
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
            if (p[0] !== this.join) {
               if (this.pair.otherRecords?.includes(p[0])) {
                  throw new Refusal(
                     `the query reads \`${p[0]}\`, another inlined join_many, which this pair table's rows do not hold`,
                  );
               }
               this.throughJoin(p);
            }
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
 * For each measure of a source (its compiled fields; `pair` names the inlined
 * path, as {@link FlatPair} does but with no flags), the levels its
 * aggregates read, through the measures it reads by name; a measure that
 * cannot be read at all (an ungrouped or windowed aggregate, one through
 * another join, one reading two levels in one aggregate, one above the leaf
 * filtered by a deeper level) is absent. `pair` may be a join's name, for a
 * one-hop path.
 */
export function pairMeasureLevels(
   sourceFields: unknown,
   pair: string | Pick<FlatPair, "join" | "hops">,
): Map<string, Set<PairLevel>> {
   const path = typeof pair === "string" ? { join: pair } : pair;
   const fields = fieldsByName(sourceFields);
   const walker = new Walker(fields, path);
   const leaf = depthOf(path);
   const out = new Map<string, Set<PairLevel>>();
   for (const [name, f] of fields) {
      if (!isAggregateField(f) || !f.e || typeof f.e !== "object") continue;
      const levels = new Set<PairLevel>();
      try {
         walker.aggregates(
            f.e as Node,
            { flagged: undefined, filterLevel: 0, inAggregate: false },
            (level, ctx) => {
               levels.add(level);
               // A filter reading a deeper level on an aggregate above the
               // leaf keeps each of its rows on its flagged row alone.
               if (level < leaf && ctx.filterLevel > level) {
                  throw new Refusal(
                     "an aggregate above the leaf filtered by a deeper level",
                  );
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
 * For each measure of a source that aggregates at one level of the inlined
 * path (as {@link pairMeasureLevels} places it, level 1 or deeper), what its
 * level's presence field does for it on the flat rows:
 *
 *  - `needed`: some aggregate in it reads the null-extended row of a row with
 *    nothing at that level, which live skips (a `count()`, or a `sum` or
 *    `avg` of a value that is not a stored column of the level: a constant,
 *    `??`, a computed column, a column of a level above it), and every
 *    aggregate in it is a `count()`, `sum` or `avg`, which live takes over
 *    the level's own rows, so re-declared under the field it is exact;
 *  - `unneeded`: it reads no such row (a sum of a stored column, a `max`);
 *  - `unsafe`: it needs the field, but also holds a `min`, `max` or distinct
 *    count, which live reads that row for, so the field would make that part
 *    wrong.
 *
 * A measure over the source's own fields, or over more than one level, or one
 * {@link pairMeasureLevels} cannot read, is absent. `pair` names the inlined
 * path and the computed fields the table stores.
 */
export function pairMeasurePresence(
   sourceFields: unknown,
   pair: Pick<FlatPair, "join" | "hops" | "computedFields">,
): Map<string, "needed" | "unneeded" | "unsafe"> {
   const fields = fieldsByName(sourceFields);
   const walker = new Walker(fields, pair);
   const out = new Map<string, "needed" | "unneeded" | "unsafe">();
   for (const [name, levels] of pairMeasureLevels(sourceFields, pair)) {
      if (levels.size !== 1 || [...levels][0] === 0) continue;
      const f = fields.get(name);
      if (!f?.e || typeof f.e !== "object") continue;
      let needed = false;
      let unguarded = false;
      try {
         walker.aggregates(
            f.e as Node,
            { flagged: undefined, filterLevel: 0, inAggregate: false },
            (level, _ctx, aggregate) => {
               const fn = (aggregate as { function?: unknown }).function;
               if (fn !== "count" && fn !== "sum" && fn !== "avg") {
                  unguarded = true;
               } else if (!storedColumnOf(pair, level, aggregate)) {
                  needed = true;
               }
            },
            0,
         );
      } catch (e) {
         if (e instanceof Refusal) continue;
         throw e;
      }
      out.set(name, !needed ? "unneeded" : unguarded ? "unsafe" : "needed");
   }
   return out;
}

/**
 * Whether an aggregate at `level` reads a stored column of that level, not a
 * computed one: null on the null-extended row of a row with nothing at that
 * level, where its key is null too. A `count()` reads no column.
 */
function storedColumnOf(
   pair: Pick<FlatPair, "join" | "hops" | "computedFields">,
   level: number,
   aggregate: Node,
): boolean {
   const arg = (aggregate.e ?? {}) as Node;
   const path = arg.node === "field" ? pathOf(arg) : undefined;
   return (
      path !== undefined &&
      path.length === level + 1 &&
      levelOfNames(path, pair) === level &&
      !(pair.computedFields ?? []).includes(path.join("."))
   );
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
    * A filter applied to the source outside the query (a row-level gate
    * grafted at read, {@link pairConditionReads}): the deepest level it reads
    * (`true` reads the leaf, as a gate through the join did before paths of
    * more than one hop), so aggregates above it are refused as under a source
    * `where:` that reads it.
    */
   scoped: boolean | number = false,
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
      const walker = new Walker(fields, pair);
      const scopedLevel =
         scoped === true ? walker.depth : scoped === false ? 0 : scoped;
      const sourceFilter = Math.max(
         scopedLevel,
         filtersLevel(walker, source?.filterList, 0),
      );
      // How deep the whole query reads: a level between the source and the
      // leaf aggregates as live does only when nothing reads below it.
      const read = pairQueryReadLevel(query, source, pair);
      if (read === undefined) {
         throw new Refusal("the query could not be read");
      }
      const reach = Math.max(read, sourceFilter);
      checkSegment(walker, fields, pipeline[0], pair, sourceFilter, reach, 0);
      return undefined;
   } catch (e) {
      if (e instanceof Refusal) return e.message;
      throw e;
   }
}

function filtersLevel(
   walker: Walker,
   filterList: unknown,
   depth: number,
): number {
   let level = 0;
   for (const c of Array.isArray(filterList) ? (filterList as Node[]) : []) {
      const f = c as Node & { expressionType?: unknown };
      if (f.expressionType !== undefined && f.expressionType !== "scalar") {
         continue;
      }
      level = Math.max(
         level,
         deepest(walker.readsOf((c.e ?? {}) as Node, depth + 1)),
      );
   }
   return level;
}

/**
 * One stage of a query (or of a nest in it): every value it groups by, every
 * filter, then every aggregate, under what its enclosing stages group and
 * filter by (`scoped`, the deepest level they read). `reach` is the deepest
 * level the whole query reads.
 */
function checkSegment(
   outer: Walker,
   sourceFields: Map<string, Field>,
   segment: Segment,
   pair: FlatPair,
   scoped: number,
   reach: number,
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
      local.size === sourceFields.size ? outer : new Walker(local, pair);
   const queryFields = Array.isArray(segment.queryFields)
      ? (segment.queryFields as (Field & { path?: unknown })[])
      : [];
   let grouped = scoped;
   const aggregates: Node[] = [];
   const nests: Segment[][] = [];
   for (const qf of queryFields) {
      if (qf.type === "fieldref") {
         const p = pathOf(qf as Node);
         if (!p) throw new Refusal("a field reference that cannot be read");
         if (p[0] === pair.join) {
            grouped = Math.max(grouped, levelOfNames(p, pair));
            continue;
         }
         if (pair.otherRecords?.includes(p[0])) {
            throw new Refusal(
               `the query reads \`${p[0]}\`, another inlined join_many, which this pair table's rows do not hold`,
            );
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
         grouped = Math.max(grouped, deepest(walker.readsOfPath(p, depth + 1)));
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
      grouped = Math.max(
         grouped,
         deepest(walker.readsOf(qf.e as Node, depth + 1)),
      );
   }
   let filtered = grouped;
   for (const c of Array.isArray(segment.filterList)
      ? (segment.filterList as (Node & { expressionType?: unknown })[])
      : []) {
      if (c.expressionType === "aggregate") {
         aggregates.push((c.e ?? {}) as Node);
      } else {
         filtered = Math.max(
            filtered,
            deepest(walker.readsOf((c.e ?? {}) as Node, depth + 1)),
         );
      }
   }
   const leaf = walker.depth;
   for (const a of aggregates) {
      walker.aggregates(
         a,
         { flagged: undefined, filterLevel: 0, inAggregate: false },
         (level, ctx, aggregate) => {
            if (level === leaf) {
               if (ctx.flagged !== undefined) {
                  throw new Refusal(
                     "an aggregate through the inlined join under a row flag",
                  );
               }
               const unmatched = unmatchedRowRefusal(
                  pair,
                  leaf,
                  aggregate,
                  ctx.present?.includes(leaf) ?? false,
               );
               if (unmatched !== undefined) throw new Refusal(unmatched);
               return;
            }
            if (ctx.flagged !== level) {
               throw new Refusal(
                  level === 0
                     ? "an aggregate over the source's own fields that is not one of its measures, which a pair table stores once per child"
                     : "an aggregate above the inlined path's last join that is not one of the source's measures, which a pair table stores once per row below it",
               );
            }
            if (Math.max(filtered, ctx.filterLevel) > level) {
               throw new Refusal(
                  level === 0
                     ? "an aggregate over the source's own fields, grouped or filtered by the inlined join's fields"
                     : "an aggregate above the inlined path's last join, grouped or filtered by a level below it",
               );
            }
            const present = ctx.present?.includes(level) ?? false;
            if (level > 0) {
               const unmatched = unmatchedRowRefusal(
                  pair,
                  level,
                  aggregate,
                  present,
               );
               if (unmatched !== undefined) throw new Refusal(unmatched);
            }
            if (
               level > 0 &&
               reach > level &&
               !besideDeeperExact(pair, level, aggregate, present)
            ) {
               throw new Refusal(
                  "an aggregate above the inlined path's last join beside fields below it, which live aggregates by that join's key",
               );
            }
         },
         depth + 1,
      );
   }
   for (const pipeline of nests) {
      if (pipeline.length === 0) throw new Refusal("a nest with no stage");
      checkSegment(
         walker,
         local,
         pipeline[0],
         pair,
         filtered,
         reach,
         depth + 1,
      );
   }
}

/**
 * Why an aggregate at a level of the inlined path (the leaf, or a middle
 * level under its row flag, which also marks the null-extended row of a
 * parent with no row there) reads that row where live skips it, or
 * undefined. Since malloydata/malloy#3208 live's `count()`, `sum` and `avg`
 * at a join skip the rows where its key is NULL: a count there counts that
 * level's rows only, which the pair table's rows cannot say apart from the
 * null-extended one; a sum or average is exact when its argument is a stored
 * column of the level, null on that row, and not otherwise (a constant, a
 * computed column, `??`). Under the level's presence field (`present`) the
 * flat rows are that level's own rows, as live's are, so a count, sum or
 * average there is exact whatever it reads. A `min`, `max` or distinct count
 * reads that row live too (Malloy does not guard them), so it is exact as it
 * stands, and under the presence field only of a stored column of the level,
 * which is null on every row the field drops (the field is declared only
 * where the build checked the level's key never null on a row of its own).
 * At the leaf, a count, sum or average is exact only where the leaf's key
 * check passed ({@link FlatPair.leafKeyChecked}).
 */
function unmatchedRowRefusal(
   pair: FlatPair,
   level: number,
   aggregate: Node,
   present: boolean,
): string | undefined {
   const fn = (aggregate as { function?: unknown }).function;
   if (
      level === depthOf(pair) &&
      !pair.leafKeyChecked &&
      (fn === "count" || fn === "sum" || fn === "avg")
   ) {
      return "a count, sum or average through the inlined join whose key is not known unique and never null, which live reads by that key";
   }
   if (fn === "min" || fn === "max" || fn === "distinct") {
      return !present || storedColumnOf(pair, level, aggregate)
         ? undefined
         : "a minimum, maximum or distinct count through the inlined join under the join's presence, which would skip the row live reads for a parent with none";
   }
   if (present && (fn === "count" || fn === "sum" || fn === "avg")) {
      return undefined;
   }
   if (fn === "count") {
      return "a count through the inlined join, which live takes over the join's own rows and the pair table's row for a parent with none would add to";
   }
   if (
      (fn === "sum" || fn === "avg") &&
      storedColumnOf(pair, level, aggregate)
   ) {
      return undefined;
   }
   return "an aggregate through the inlined join of a value that is not null on the pair table's row for a parent with none, which live skips";
}

/**
 * Whether a middle level's aggregate (under its flag, grouped and filtered by
 * nothing below it) equals live's beside fields below it, where live
 * aggregates that level symmetrically by its primary key: the level and
 * every level above it is one whose rows have one parent row each
 * ({@link FlatPair.oneParentLevels}), so its flag marks one row per row of
 * the level in any group; and the aggregate reads nothing on a childless
 * row's null-extended row, which live's symmetric aggregate by key skips:
 * under the level's presence field (`present`), a count, sum or average of
 * anything, since the field drops that row; otherwise a sum or an average of
 * one of the level's stored columns (not a computed one), null there. A
 * count without the field is not: the flag marks that null row. Nor is a
 * computed column, which need not be null on that row, where whether live's
 * symmetric aggregate reads a null key's row depends on the dialect
 * (DuckDB's hash of a null key is not null).
 */
function besideDeeperExact(
   pair: FlatPair,
   level: number,
   aggregate: Node,
   present: boolean,
): boolean {
   const proven = pair.oneParentLevels ?? [];
   for (let l = 1; l <= level; l++) if (!proven.includes(l)) return false;
   // The aggregates Malloy places at a join are sums, averages and counts.
   const fn = (aggregate as { function?: unknown }).function;
   if (present && (fn === "count" || fn === "sum" || fn === "avg")) {
      return true;
   }
   // A count reads no field (`items.count()`), so the column check refuses
   // it.
   return storedColumnOf(pair, level, aggregate);
}

function pipelineOf(f: Field): Segment[] {
   return Array.isArray(f.pipeline) ? (f.pipeline as Segment[]) : [];
}

/**
 * The deepest level of the inlined path a compiled query reads: a field, an
 * aggregate, a filter, a group-by or a nest along it, directly or through the
 * definitions it reads by name (a dimension, a measure, a view), in its first
 * stage, a nest's, or the source's own `where:`; 0 when it reads nothing
 * through the path. Undefined when the query cannot be read (a reference that
 * does not resolve, a node of no known shape): the caller then treats it as
 * not answerable from the pair table. `query` and `source` are as
 * {@link flatPairQueryRefusal} takes them; `pair` may be a join's name, for a
 * one-hop path. Names alone decide, so it reads a join of that name the same
 * way it reads a record.
 */
export function pairQueryReadLevel(
   query: { pipeline?: unknown } | undefined,
   source: { fields?: unknown; filterList?: unknown } | undefined,
   pair: string | Pick<FlatPair, "join" | "hops">,
): number | undefined {
   const path = typeof pair === "string" ? { join: pair } : pair;
   try {
      const fields = fieldsByName(source?.fields);
      const pipeline = Array.isArray(query?.pipeline)
         ? (query?.pipeline as Segment[])
         : [];
      if (pipeline.length === 0) return undefined;
      const scan = new LevelScan(path);
      let level = 0;
      for (const c of Array.isArray(source?.filterList)
         ? (source?.filterList as unknown[])
         : []) {
         level = Math.max(level, scan.value(c, fields, 0));
      }
      return Math.max(level, scan.stage(pipeline[0], fields, 0));
   } catch (e) {
      if (e instanceof Refusal) return undefined;
      throw e;
   }
}

/**
 * What a compiled filter condition (a row-level gate's, lifted from its
 * probe) on `source` reads of a pair table: the deepest level of the inlined
 * path, and whether it reads through a join that fans out the pair rows (a
 * join_many other than the path, or another block's record), which no
 * aggregate over the flat rows survives. Undefined when it cannot be read;
 * the caller then counts it as reading through such a join.
 */
export function pairConditionReads(
   condition: unknown,
   source: { fields?: unknown } | undefined,
   pair: string | Pick<FlatPair, "join" | "hops" | "otherRecords">,
): { level: number; fansOut: boolean } | undefined {
   const path = typeof pair === "string" ? { join: pair } : pair;
   try {
      const scan = new LevelScan(path);
      const level = scan.value(condition, fieldsByName(source?.fields), 0);
      return { level, fansOut: scan.fansOut };
   } catch (e) {
      if (e instanceof Refusal) return undefined;
      throw e;
   }
}

/**
 * The walk behind {@link pairQueryReadLevel}: every node of a stage, each
 * field reference resolved by name through its definition, each nest by its
 * first stage. Later stages read the first stage's output, not the source.
 * It also notes a read through a join that fans out the pair rows
 * ({@link pairConditionReads}).
 */
class LevelScan {
   /** Each definition's answer, read once however often it is named. */
   private readonly resolved = new WeakMap<object, number>();
   /** The joins whose condition is being read, which read their own fields. */
   private readonly joining = new Set<string>();
   /** Whether a read went through a join that fans out the pair rows. */
   fansOut = false;

   constructor(
      private readonly pair: Pick<FlatPair, "join" | "hops" | "otherRecords">,
   ) {}

   stage(segment: unknown, fields: Map<string, Field>, depth: number): number {
      if (depth > MAX_DEPTH) throw new Refusal("a query nests too deep");
      if (!segment || typeof segment !== "object") {
         throw new Refusal("a stage that cannot be read");
      }
      const s = segment as Segment;
      const local = new Map(fields);
      for (const [n, f] of fieldsByName(s.extendSource)) local.set(n, f);
      let level = 0;
      for (const [k, v] of Object.entries(s)) {
         // A stage's own `extend:` is read where the stage names it; its
         // output schema is what it produces, not what it reads.
         if (k === "extendSource" || k === "outputStruct") continue;
         level = Math.max(level, this.value(v, local, depth + 1));
      }
      return level;
   }

   value(v: unknown, fields: Map<string, Field>, depth: number): number {
      if (depth > MAX_DEPTH) throw new Refusal("an expression nests too deep");
      if (Array.isArray(v)) {
         let level = 0;
         for (const x of v) {
            level = Math.max(level, this.value(x, fields, depth + 1));
         }
         return level;
      }
      if (!v || typeof v !== "object") return 0;
      const n = v as Node & Field & { type?: unknown };
      let level = 0;
      if (Array.isArray(n.structPath) && n.structPath.length > 0) {
         if (n.structPath[0] === this.pair.join) {
            level = levelOfStruct(n.structPath, this.pair);
         } else {
            this.notePath(n.structPath, fields);
         }
      }
      if (n.node === "field" || n.type === "fieldref") {
         const p = pathOf(n);
         if (!p) throw new Refusal("a field reference that cannot be read");
         return Math.max(level, this.path(p, fields, depth + 1));
      }
      if (n.type === "turtle" && n.pipeline !== undefined) {
         const nest = pipelineOf(n);
         if (nest.length === 0) throw new Refusal("a nest with no stage");
         return Math.max(level, this.stage(nest[0], fields, depth + 1));
      }
      for (const x of Object.values(v)) {
         level = Math.max(level, this.value(x, fields, depth + 1));
      }
      return level;
   }

   /**
    * Note a path through a join that fans out the pair rows: another block's
    * record at its head, or a join_many or cross join at any step of it,
    * stepped through from the queried source (a join_many reached through a
    * join_one fans out as much as one on the source). `p` names joins only,
    * or ends at a field, which is no join.
    */
   private notePath(p: readonly unknown[], fields: Map<string, Field>): void {
      if (typeof p[0] === "string" && this.pair.otherRecords?.includes(p[0])) {
         this.fansOut = true;
         return;
      }
      let scope = fields;
      for (const name of p) {
         if (typeof name !== "string") return;
         const f = scope.get(name) as
            | (Field & { fields?: unknown })
            | undefined;
         if (!f) return;
         if (f.join === "many" || f.join === "cross") {
            this.fansOut = true;
            return;
         }
         scope = fieldsByName(f.fields);
      }
   }

   private path(
      p: string[],
      fields: Map<string, Field>,
      depth: number,
   ): number {
      if (p[0] === this.pair.join) return levelOfNames(p, this.pair);
      const f = fields.get(p[0]);
      if (!f) throw new Refusal(`the query reads \`${p[0]}\`, unresolved`);
      if (p.length > 1) {
         this.notePath(p.slice(0, -1), fields);
         // Through another join: the joined source's own field, reached by
         // that join's condition, which may read the inlined join (its reads
         // of the join's own fields are the join's, not the parent's).
         if (this.joining.has(p[0])) return 0;
         const on = (f as { onExpression?: unknown }).onExpression;
         if (on === undefined) return 0;
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
      if (f.e === undefined) return 0;
      const known = this.resolved.get(f);
      if (known !== undefined) return known;
      const reads = this.value(f.e, fields, depth + 1);
      this.resolved.set(f, reads);
      return reads;
   }
}

/** What {@link flatPairQueryVerdict} decides for one query over a pair table. */
type FlatPairVerdict =
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
   scoped: boolean | number = false,
): FlatPairVerdict {
   const level = pairQueryReadLevel(query, source, pair);
   if (level === undefined) {
      return {
         kind: "refused",
         reason:
            "the query could not be read to tell whether it reads the inlined join",
      };
   }
   if (level === 0) return { kind: "parent_only" };
   const reason = flatPairQueryRefusal(query, source, pair, scoped);
   return reason === undefined
      ? { kind: "served" }
      : { kind: "refused", reason };
}
