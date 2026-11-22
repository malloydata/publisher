// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which strings a query's result depends on the order or the equality of,
 * for the check that a stored table answers as its source does
 * (`gate_collation.ts` judges them, the router asks): a `storage=` copy by
 * the source's collation, and every stored table, a database one included,
 * by whether its stored columns still compare as the source's do.
 *
 * A `storage=` table is read in a DuckLake destination, which compares
 * strings byte-exact. Live compares the source column under its own
 * collation. Under a deterministic one (a Postgres `en_US` default, an ICU
 * locale) equality is byte equality, so grouping, counting distinct values
 * and equality hold on the copy, but ordering does not: an `order_by`, a
 * range comparison and a `min`/`max` of a string differ. Under a
 * nondeterministic one (a case- or accent-insensitive ICU collation, a
 * DuckDB `NOCASE` column) equality differs too.
 *
 * So the query's result reads are split by what they need:
 *
 * - `ordered`: a string the result's order or values depend on the order
 *   of. A stage's ordering, explicit or Malloy's default (written into the
 *   IR as the stage's `orderBy`: the first time or aggregate field
 *   descending, else the first field ascending), counted where it decides
 *   something: the last stage of the query or of a nest, a stage with a
 *   `limit`, and a stage whose window functions follow its order. A window
 *   function's own `order_by:`, `min`/`max`, `greatest`/`least`, a range
 *   comparison (`<`, `>`, `<=`, `>=`), and an aggregate function that is not
 *   known to ignore its argument's order.
 * - `grouped`: a string the result groups by or compares for equality: a
 *   `group_by:`, `count(x)`, `=`, `!=`, `in`, a pattern, a `pick`'s value
 *   against its `when:`s, a window's `partition_by:`, and a join's `on`.
 *
 * A string derived from a column (`lower`, `concat`, `pick`, a dimension)
 * compares under that column's collation, so the derived value's use is
 * read as a use of every column it reads. Filters a query writes (`where:`,
 * `having:`, a filtered measure's) are judged by the query-filter check
 * (`queryFilterReadsOfQuery`), and are read here only for the joins they
 * reach. Anything this cannot follow is unresolved: the caller fails closed.
 */

import type { QueryMaterializer } from "@malloydata/malloy";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import { capabilitiesOf, type Capabilities } from "../domain/dialect";
import type { GateReads } from "./gate_reads_types";

/** How a value is used by the result. */
type Use = "order" | "group" | "value";

/**
 * Whether a walk judges what an expression does (it is computed when the
 * stored table is read), or only traces which columns a value comes from
 * (it was computed before: a stored stage, or a stage judged on its own).
 */
type Mode = "judge" | "trace";

interface FieldLike {
   name?: unknown;
   as?: unknown;
   e?: unknown;
   join?: unknown;
   type?: unknown;
   path?: unknown;
   pipeline?: unknown;
   expressionType?: unknown;
   onExpression?: unknown;
   primaryKey?: unknown;
   sourceID?: unknown;
   query?: unknown;
}

/** One stage of a compiled query, as the walk reads it. */
interface Stage {
   type?: unknown;
   indexFields?: unknown;
   queryFields?: unknown;
   filterList?: unknown;
   orderBy?: unknown;
   limit?: unknown;
   isRepeated?: unknown;
}

/** Where a field path is read: a source's fields, or a stage's outputs. */
type Scope =
   | {
        readonly kind: "source";
        readonly source: unknown;
        /** The join path from the entry point (or the inner base) to `source`. */
        readonly prefix: readonly string[];
        /** The path is read from the query's entry point. */
        readonly entry: boolean;
     }
   | {
        readonly kind: "stage";
        readonly stage: Stage;
        /** What the stage reads. */
        readonly input: Scope;
     };

/** The string reads a query's result depends on, split by what they need. */
export interface ResultReads {
   /** Strings whose order the result depends on: binary collations only. */
   readonly ordered: GateReads;
   /** Strings the result groups or compares by equality: deterministic ones too. */
   readonly grouped: GateReads;
   /** How the entry point's dialect reads a string column's collation. */
   readonly stringCollation: Capabilities["stringCollation"];
}

const RANGE_OPERATORS = new Set([">", "<", ">=", "<="]);
const EQUALITY_OPERATORS = new Set(["=", "!=", "like", "!like"]);
/**
 * Functions whose value is chosen by ordering their arguments: a scalar
 * `greatest`/`least`, and a window's `min`/`max` over its frame.
 */
const ORDERING_FUNCTIONS = new Set([
   "greatest",
   "least",
   "min",
   "max",
   "min_window",
   "max_window",
   "min_cumulative",
   "max_cumulative",
]);
/**
 * Functions that compare their string arguments for equality, or search one
 * in another, which a nondeterministic collation does otherwise.
 */
const EQUALITY_FUNCTIONS = new Set([
   "nullif",
   "starts_with",
   "ends_with",
   "strpos",
   "replace",
   "split_part",
   "regexp_extract",
   "regexp_replace",
]);
/**
 * Functions whose value (a number) does not depend on how a collation
 * compares their string argument: its length, its first code point.
 */
const COLLATION_FREE_FUNCTIONS = new Set([
   "length",
   "byte_length",
   "ascii",
   "unicode",
]);
/** Aggregate functions known not to order their argument. */
const UNORDERED_AGGREGATES = new Set([
   "string_agg",
   "count",
   "sum",
   "avg",
   "stddev",
   "variance",
]);

/** A known type that is not a string. Unknown is not. */
function nonStringType(t: unknown): boolean {
   return typeof t === "string" && t !== "string";
}

function asArray(x: unknown): unknown[] {
   return Array.isArray(x) ? x : [];
}

function activeName(f: FieldLike): string {
   return String(f.as ?? f.name);
}

function fieldsOf(struct: unknown): Map<string, FieldLike> {
   const out = new Map<string, FieldLike>();
   for (const f of asArray(
      (struct as { fields?: unknown } | undefined)?.fields,
   )) {
      out.set(activeName(f as FieldLike), f as FieldLike);
   }
   return out;
}

/** A stage's output fields, by the name each goes by. */
function stageFields(stage: Stage): Map<string, FieldLike> {
   const out = new Map<string, FieldLike>();
   for (const qf of asArray(stage.queryFields) as FieldLike[]) {
      if (qf.type === "fieldref" && Array.isArray(qf.path)) {
         const p = qf.path as string[];
         out.set(p[p.length - 1], qf);
      } else if (qf.name !== undefined) {
         out.set(activeName(qf), qf);
      }
   }
   return out;
}

/** A stage's output names, in order (an `order_by: 2` names the second). */
function stageOutputNames(stage: Stage): string[] {
   return [...stageFields(stage).keys()];
}

function isAnalytic(t: unknown): boolean {
   return typeof t === "string" && t.includes("analytic");
}

function isAggregate(t: unknown): boolean {
   return t === "aggregate" || t === "ungrouped_aggregate";
}

/**
 * Whether a cast makes a string: a cast to a string, or a raw cast to a
 * type that may be one.
 */
function castMakesString(n: Record<string, unknown>): boolean {
   const dst = (n.dstType as { type?: unknown } | undefined)?.type;
   if (dst !== undefined) return dst === "string";
   return !/^\s*(big|small|tiny)?(int|integer|numeric|decimal|real|double|float|bool|boolean|date|time|timestamp|timestamptz)\b/i.test(
      String(n.dstSQLType ?? ""),
   );
}

/** A string column a value takes its collation from, and whether its path is the entry point's. */
interface Col {
   readonly path: readonly string[];
   readonly entry: boolean;
   /**
    * Every name the value goes by on the way from the column to the read (the
    * column's, a rename's, a dimension's, a stage output's), any of which a
    * stored table may hold it under.
    */
   readonly names: readonly string[];
   /**
    * The column's source is known to be read live, never from a stored
    * table ({@link ResultReadWalk.knownLive}).
    */
   readonly live: boolean;
}

/**
 * Where a value's collation comes from, as Postgres derives it
 * (`assign_collations_walker`): one or more string columns (implicit), the
 * database's default (a literal, a given, a string made from no string), or
 * nothing (not a string).
 */
type Src =
   | { readonly kind: "none" }
   | { readonly kind: "default" }
   | { readonly kind: "columns"; readonly cols: ReadonlyMap<string, Col> };

const NONE: Src = { kind: "none" };
const DEFAULT: Src = { kind: "default" };

/**
 * The collation of a value made from `srcs` together (a `concat`, a `pick`'s
 * values, a comparison's operands): a column's outranks the default, and
 * the default outranks nothing.
 */
function combine(srcs: readonly Src[]): Src {
   const cols = new Map<string, Col>();
   let dflt = false;
   for (const s of srcs) {
      if (s.kind === "columns") for (const [k, c] of s.cols) cols.set(k, c);
      else if (s.kind === "default") dflt = true;
   }
   if (cols.size > 0) return { kind: "columns", cols };
   return dflt ? DEFAULT : NONE;
}

/** The column `path` (read from the entry point or not), as a {@link Src}. */
function column(path: readonly string[], entry: boolean, live: boolean): Src {
   return {
      kind: "columns",
      cols: new Map([
         [
            `${entry ? "e" : "x"}\u0000${path.join("\u0000")}`,
            { path, entry, names: [path[path.length - 1]], live },
         ],
      ]),
   };
}

/** `src` with `name` among the names each of its columns goes by. */
function alias(src: Src, name: unknown): Src {
   if (src.kind !== "columns" || typeof name !== "string") return src;
   const cols = new Map<string, Col>();
   for (const [k, c] of src.cols) {
      cols.set(
         k,
         c.names.includes(name) ? c : { ...c, names: [...c.names, name] },
      );
   }
   return { kind: "columns", cols };
}

/** Nodes whose value is not a string, whatever their operands are. */
const NON_STRING_NODES = new Set([
   "and",
   "or",
   "not",
   "is-null",
   "is-not-null",
   "+",
   "-",
   "*",
   "/",
   "%",
   "unary-",
   "trunc",
   "extract",
   "delta",
   "timeDiff",
]);

/** A path's records, by its key, and whether each is read from the entry point. */
class Reads {
   readonly paths = new Map<string, readonly string[]>();
   /** Each entry path's names, by its key. */
   readonly names = new Map<string, Set<string>>();
   /** The keys of entry paths every read of which is of a known-live source. */
   readonly live = new Map<string, boolean>();
   /** A read that is not a path from the entry point (an inner query's base). */
   elsewhere = false;
   /** A string under the database's default collation was compared. */
   literal = false;
   /** The columns one value combines, which must hold one collation. */
   readonly same: (readonly string[])[][] = [];

   consume(src: Src): void {
      if (src.kind === "default") {
         this.literal = true;
         return;
      }
      if (src.kind !== "columns") return;
      const group: (readonly string[])[] = [];
      for (const c of src.cols.values()) {
         if (!c.entry) this.elsewhere = true;
         else {
            const key = c.path.join("\u0000");
            this.paths.set(key, c.path);
            let names = this.names.get(key);
            if (names === undefined) {
               names = new Set();
               this.names.set(key, names);
            }
            for (const n of c.names) names.add(n);
            this.live.set(key, (this.live.get(key) ?? true) && c.live);
            group.push(c.path);
         }
      }
      if (src.cols.size > 1) this.same.push(group);
   }

   get empty(): boolean {
      return this.paths.size === 0 && !this.elsewhere && !this.literal;
   }
}

/** The walk behind {@link resultReadsOfQuery}. */
class ResultReadWalk {
   readonly ordered = new Reads();
   readonly grouped = new Reads();
   unresolved = false;
   private steps = 0;
   /** Each join's `on` and each inner query, walked once. */
   private readonly walked = new Set<unknown>();

   constructor(
      private readonly modelDef: unknown,
      private readonly stored: ReadonlySet<string>,
      private readonly storedSources: ReadonlySet<string>,
   ) {}

   private tooDeep(): boolean {
      return ++this.steps > 8192;
   }

   /** A value of collation `src` compared as `use` needs. */
   private consume(src: Src, use: Use): void {
      if (use === "order") this.ordered.consume(src);
      else if (use === "group") this.grouped.consume(src);
   }

   /**
    * Whether the answer reads `source`'s own table: its `sourceID` is
    * stored, or it extends a stored query-shaped source and carries that
    * source's query unchanged, so its stages were computed by the build.
    */
   private isStored(source: unknown): boolean {
      const s = source as { sourceID?: unknown; query?: unknown } | undefined;
      if (typeof s?.sourceID === "string" && this.stored.has(s.sourceID)) {
         return true;
      }
      if (s?.query === undefined) return false;
      if (this.storedQueries === undefined) {
         this.storedQueries = new Set();
         const contents =
            (
               this.modelDef as
                  | { contents?: Record<string, unknown> }
                  | undefined
            )?.contents ?? {};
         for (const c of Object.values(contents)) {
            const d = c as { sourceID?: unknown; query?: unknown } | undefined;
            if (
               typeof d?.sourceID === "string" &&
               this.stored.has(d.sourceID) &&
               d.query !== undefined
            ) {
               this.storedQueries.add(JSON.stringify(d.query));
            }
         }
      }
      return this.storedQueries.has(JSON.stringify(s.query));
   }

   /** The queries of the stored query-shaped sources the model names. */
   private storedQueries?: Set<string>;

   /**
    * Whether a column of `owner` is known to be read live, so no stored
    * table's collation can change it: the answer names the sources whose
    * tables it reads by ID, and `owner` is a table or SQL source of its own
    * whose ID is not one of them (the parent beside a joined rollup, say).
    * A query-shaped source, one with no ID, or an answer that names no
    * source is not known live: its columns are judged. The stored-side
    * check still judges such a column wherever a stored table holds it.
    */
   knownLive(owner: unknown): boolean {
      if (this.storedSources.size === 0) return false;
      const s = owner as
         | { sourceID?: unknown; query?: unknown; type?: unknown }
         | undefined;
      if (typeof s?.sourceID !== "string" || s.query !== undefined) {
         return false;
      }
      if (this.storedSources.has(s.sourceID) || this.isStored(owner)) {
         return false;
      }
      return s.type === "table" || s.type === "sql_select";
   }

   /** The source a query-shaped source's query runs on, or undefined. */
   private baseOf(query: { structRef?: unknown }): unknown {
      const ref = query.structRef;
      const base =
         typeof ref === "string"
            ? (
                 this.modelDef as
                    | { contents?: Record<string, unknown> }
                    | undefined
              )?.contents?.[ref]
            : ref;
      return base === null ? undefined : base;
   }

   /** Every stage of `pipeline` from `fromStage` on, read from `input`. */
   pipeline(
      stages: readonly Stage[],
      input: Scope,
      options: {
         fromStage: number;
         /** A stage only whose ordering is judged: a stored result's own. */
         orderOnly?: number;
         /** An inner query of a source: its outer query orders the rows. */
         inner?: boolean;
      },
   ): void {
      let scope = input;
      stages.forEach((stage, i) => {
         const last = i === stages.length - 1 && options.inner !== true;
         if (i === options.orderOnly) {
            this.ordering(stage, scope, last, false);
         } else if (i >= options.fromStage) {
            this.stage(stage, scope, last);
         }
         scope = { kind: "stage", stage, input: scope };
      });
   }

   /** One stage: its fields, its nests, its filters' joins, its ordering. */
   private stage(stage: Stage, input: Scope, last: boolean): void {
      if (stage.type === "index") {
         // An `index:` groups each field's values and ranges them: each is
         // read as ordered, the stricter need.
         for (const qf of asArray(stage.indexFields) as FieldLike[]) {
            this.consume(this.queryField(qf, input, "judge", stage), "order");
         }
         for (const f of asArray(stage.filterList)) {
            this.expr((f as { e?: unknown }).e, input, "judge", stage);
         }
         return;
      }
      if (stage.type !== "reduce" && stage.type !== "project") {
         this.unresolved = true;
         return;
      }
      let analytic = false;
      for (const qf of asArray(stage.queryFields) as FieldLike[]) {
         if (qf.type === "turtle") {
            // A nest reads the stage's input; its rows are ordered arrays.
            this.pipeline(asArray(qf.pipeline) as Stage[], input, {
               fromStage: 0,
            });
            continue;
         }
         const kind = this.kindOf(qf, input);
         if (isAnalytic(kind)) analytic = true;
         const src = this.queryField(qf, input, "judge", stage);
         if (
            stage.type === "reduce" &&
            !isAggregate(kind) &&
            !isAnalytic(kind)
         ) {
            this.consume(src, "group");
         }
      }
      for (const f of asArray(stage.filterList)) {
         this.expr((f as { e?: unknown }).e, input, "judge", stage);
      }
      this.ordering(stage, input, last, analytic);
   }

   /**
    * The stage's ordering, where it decides something: the last stage (of
    * the query or a nest), a stage with a `limit`, or one whose window
    * functions follow it. A stage that returns one row orders nothing.
    */
   private ordering(
      stage: Stage,
      input: Scope,
      last: boolean,
      analytic: boolean,
   ): void {
      const orderBy = asArray(stage.orderBy);
      if (orderBy.length === 0 || stage.isRepeated === false) return;
      if (!last && stage.limit === undefined && !analytic) return;
      const fields = stageFields(stage);
      const names = stageOutputNames(stage);
      for (const o of orderBy) {
         const key = (o as { field?: unknown }).field;
         const name = typeof key === "number" ? names[key - 1] : key;
         const qf = typeof name === "string" ? fields.get(name) : undefined;
         if (qf === undefined) {
            this.unresolved = true;
            continue;
         }
         // The key is the stage's own output, judged with its fields (or
         // computed by a stored result's build): only its collation is read.
         this.consume(this.queryField(qf, input, "trace", stage), "order");
      }
   }

   /** Whether a stage field is a dimension, a measure or a calculation. */
   private kindOf(qf: FieldLike, input: Scope): unknown {
      if (qf.type !== "fieldref") return qf.expressionType ?? "scalar";
      if (input.kind === "stage") return "scalar";
      const path = asArray(qf.path) as string[];
      let at: unknown = input.source;
      let f: FieldLike | undefined;
      for (const name of path) {
         f = fieldsOf(at).get(name);
         if (f === undefined) return undefined;
         at = f;
      }
      return f?.expressionType ?? "scalar";
   }

   /** One of a stage's fields, read from the stage's input: its collation. */
   private queryField(
      qf: FieldLike,
      input: Scope,
      mode: Mode,
      stage: Stage,
   ): Src {
      // The stage's output name is one more the value goes by (a stored
      // result's table holds its outputs by it).
      const output = qf.as ?? qf.name;
      if (qf.type === "fieldref" && Array.isArray(qf.path)) {
         return alias(this.field(input, qf.path as string[], mode), output);
      }
      if (qf.type === "turtle") return NONE;
      if (qf.e !== undefined) {
         if (mode === "trace" && nonStringType(qf.type)) return NONE;
         const src = this.expr(qf.e, input, mode, stage);
         return nonStringType(qf.type) ? NONE : alias(src, output);
      }
      this.unresolved = true;
      return NONE;
   }

   /** A field path read in `scope`: the collation of its value. */
   field(scope: Scope, path: readonly string[], mode: Mode): Src {
      if (this.tooDeep() || path.length === 0) {
         this.unresolved = true;
         return NONE;
      }
      if (scope.kind === "stage") {
         const qf =
            path.length === 1
               ? stageFields(scope.stage).get(path[0])
               : undefined;
         if (qf === undefined) {
            this.unresolved = true;
            return NONE;
         }
         // A stage's output was computed by that stage, which is judged on
         // its own: only the collation its value carries is traced.
         return this.queryField(qf, scope.input, "trace", scope.stage);
      }
      let owner = scope.source;
      const prefix = [...scope.prefix];
      for (const name of path.slice(0, -1)) {
         const j = fieldsOf(owner).get(name);
         if (j === undefined || j.join === undefined) {
            this.unresolved = true;
            return NONE;
         }
         if (mode === "judge") this.join(owner, j, prefix, scope.entry);
         owner = j;
         prefix.push(name);
      }
      const last = path[path.length - 1];
      const f = fieldsOf(owner).get(last);
      if (f === undefined) {
         this.unresolved = true;
         return NONE;
      }
      const at: Scope = {
         kind: "source",
         source: owner,
         prefix,
         entry: scope.entry,
      };
      if (f.e !== undefined) {
         if (mode === "trace" && nonStringType(f.type)) return NONE;
         const src = this.expr(f.e, at, mode, undefined);
         return nonStringType(f.type) ? NONE : alias(src, last);
      }
      if (f.join !== undefined) return NONE;
      const query = (owner as { query?: unknown }).query;
      if (query !== undefined && !this.isStored(owner)) {
         // A stage output of a query-shaped source computed when the table
         // is read: its query is judged (once its source is reached), and
         // the output traced back.
         if (mode === "judge") this.inner(owner);
         const stages = asArray(
            (query as { pipeline?: unknown }).pipeline,
         ) as Stage[];
         const base = this.baseOf(query as { structRef?: unknown });
         if (stages.length === 0 || base === undefined) {
            this.unresolved = true;
            return NONE;
         }
         let s: Scope = {
            kind: "source",
            source: base,
            prefix: [],
            entry: false,
         };
         for (const stage of stages) s = { kind: "stage", stage, input: s };
         // A rename keeps the stage output's own name.
         return alias(
            this.field(
               s,
               [typeof f.name === "string" ? f.name : last],
               "trace",
            ),
            last,
         );
      }
      return nonStringType(f.type)
         ? NONE
         : alias(
              column([...prefix, last], scope.entry, this.knownLive(owner)),
              f.name,
           );
   }

   /**
    * A join the query reaches: its `on` compares by equality, and its
    * primary key (and its owner's) is how a fanned-out aggregate counts
    * distinct rows.
    */
   private join(
      owner: unknown,
      j: FieldLike,
      prefix: readonly string[],
      entry: boolean,
   ): void {
      if (this.walked.has(j)) return;
      this.walked.add(j);
      const at: Scope = { kind: "source", source: owner, prefix, entry };
      this.inner(j);
      if (j.onExpression !== undefined) {
         this.expr(j.onExpression, at, "judge", undefined);
      }
      const name = activeName(j);
      if (typeof j.primaryKey === "string") {
         this.consume(this.field(at, [name, j.primaryKey], "trace"), "group");
      }
      const own = (owner as { primaryKey?: unknown }).primaryKey;
      if (typeof own === "string") {
         this.consume(this.field(at, [own], "trace"), "group");
      }
   }

   /**
    * The `where:`s of `source` and of every source joined to it, however
    * deep, each judged in its own scope, used by the query or not: a chained
    * build applies them over the stored tables, and a join a measure counts
    * through (`k.count()`) reads none of its fields, so is not otherwise
    * reached.
    */
   sourceFilters(source: unknown, prefix: readonly string[], depth = 0): void {
      if (depth > 16 || this.tooDeep()) {
         this.unresolved = true;
         return;
      }
      const at: Scope = { kind: "source", source, prefix, entry: true };
      for (const f of asArray(
         (source as { filterList?: unknown }).filterList,
      )) {
         this.expr((f as { e?: unknown }).e, at, "judge", undefined);
      }
      for (const j of fieldsOf(source).values()) {
         if (j.join === undefined) continue;
         this.inner(j);
         if (j.onExpression !== undefined) {
            this.expr(j.onExpression, at, "judge", undefined);
         }
         this.sourceFilters(j, [...prefix, activeName(j)], depth + 1);
      }
   }

   /**
    * A query-shaped source's own query, computed when its base is read
    * unless its own table is: judged once, however it is reached (the entry
    * point, a join, a field).
    */
   inner(owner: unknown): void {
      const query = (owner as { query?: unknown } | undefined)?.query as
         | { structRef?: unknown; pipeline?: unknown }
         | undefined;
      if (query === undefined || this.isStored(owner)) return;
      if (this.walked.has(owner)) return;
      this.walked.add(owner);
      const base = this.baseOf(query);
      if (base === undefined) {
         this.unresolved = true;
         return;
      }
      this.pipeline(
         asArray(query.pipeline) as Stage[],
         { kind: "source", source: base, prefix: [], entry: false },
         { fromStage: 0, inner: true },
      );
   }

   /** The children of an expression node, every one. */
   private children(e: Record<string, unknown>): unknown[] {
      const out: unknown[] = [];
      if (e.e !== undefined) out.push(e.e);
      const kids = e.kids;
      if (kids !== null && typeof kids === "object") {
         for (const v of Object.values(kids)) {
            if (Array.isArray(v)) out.push(...v);
            else out.push(v);
         }
      }
      return out;
   }

   /**
    * An expression read in `scope`: the collation of its value, derived
    * bottom-up as Postgres derives it. Judged, each comparison, ordering
    * and grouping inside it is recorded as its operands' collation needs;
    * traced (computed before the table is read), only the collation is
    * derived.
    */
   expr(
      e: unknown,
      scope: Scope,
      mode: Mode,
      stage: Stage | undefined,
      depth = 0,
   ): Src {
      if (depth > 64 || this.tooDeep()) {
         this.unresolved = true;
         return NONE;
      }
      if (e === null || typeof e !== "object" || Array.isArray(e)) return NONE;
      const n = e as Record<string, unknown> & { node?: unknown };
      switch (n.node) {
         case "field":
            if (Array.isArray(n.path)) {
               return this.field(scope, n.path as string[], mode);
            }
            this.unresolved = true;
            return NONE;
         case "outputField": {
            const qf =
               stage === undefined
                  ? undefined
                  : stageFields(stage).get(String(n.name));
            if (qf === undefined) {
               this.unresolved = true;
               return NONE;
            }
            return this.queryField(qf, scope, "trace", stage!);
         }
         case "compositeField":
         case "error":
         case "spread":
         case "source-reference":
            this.unresolved = true;
            return NONE;
         case "stringLiteral":
         case "given":
            return DEFAULT;
         case "numberLiteral":
         case "regexpLiteral":
         case "filterLiteral":
         case "dateLiteral":
         case "timestampLiteral":
         case "timestamptzLiteral":
         case "true":
         case "false":
         case "null":
         case "now":
         case "parameter":
         case "function_parameter":
         case "aggregate_order_by":
         case "aggregate_limit":
         case "":
            return NONE;
      }
      return this.operation(n, scope, mode, stage, depth);
   }

   /** A node with operands: what it does with them, and its value's collation. */
   private operation(
      n: Record<string, unknown>,
      scope: Scope,
      mode: Mode,
      stage: Stage | undefined,
      depth: number,
   ): Src {
      const sub = (x: unknown) => this.expr(x, scope, mode, stage, depth + 1);
      const need = (src: Src, u: Use) => {
         if (mode === "judge") this.consume(src, u);
      };
      const kids = (n.kids ?? {}) as Record<string, unknown>;
      const node = typeof n.node === "string" ? n.node : "";
      switch (node) {
         case "aggregate": {
            const src = sub(n.e);
            if (n.function === "min" || n.function === "max") {
               need(src, "order");
               return src;
            }
            if (n.function === "distinct") need(src, "group");
            return NONE;
         }
         case "in":
            need(
               combine([sub(kids.e), ...asArray(kids.oneOf).map(sub)]),
               "group",
            );
            return NONE;
         case "inGiven":
            need(sub(n.e), "group");
            return NONE;
         case "regexpMatch":
            need(sub(kids.expr), "group");
            sub(kids.regex);
            return NONE;
         case "filterMatch":
            need(sub(kids.expr), "group");
            return NONE;
         case "filteredExpr":
            // Its filter is the query-filter check's (`gate_reads.ts`).
            return sub(kids.e);
         case "case": {
            if (kids.caseValue !== undefined) {
               need(
                  combine([
                     sub(kids.caseValue),
                     ...asArray(kids.caseWhen).map(sub),
                  ]),
                  "group",
               );
            } else if (mode === "judge") {
               // A `when:` is a condition of its own, not the value's.
               for (const w of asArray(kids.caseWhen)) sub(w);
            }
            return combine([
               ...asArray(kids.caseThen).map(sub),
               sub(kids.caseElse),
            ]);
         }
         case "cast": {
            const src = sub(n.e);
            if (!castMakesString(n)) return NONE;
            // A string cast from a string keeps its collation; one cast from
            // anything else has the default.
            return src.kind === "columns" ? src : DEFAULT;
         }
         case "function_call":
            return this.functionCall(n, scope, mode, stage, depth);
         case "genericSQLExpr": {
            // Raw SQL: what it does with its arguments is not read.
            const args = asArray(kids.args).map(sub);
            for (const a of args) need(a, "order");
            const src = combine(args);
            return src.kind === "columns" ? src : DEFAULT;
         }
      }
      if (RANGE_OPERATORS.has(node)) {
         need(combine(this.children(n).map(sub)), "order");
         return NONE;
      }
      if (EQUALITY_OPERATORS.has(node)) {
         need(combine(this.children(n).map(sub)), "group");
         return NONE;
      }
      const operands = this.children(n).map(sub);
      // Logic, arithmetic, time arithmetic: not a string. `()`,
      // `coalesce`, an ungrouping, anything else: its operands' collation.
      return NON_STRING_NODES.has(node) ? NONE : combine(operands);
   }

   /** A function call: scalar, aggregate or window. */
   private functionCall(
      n: Record<string, unknown>,
      scope: Scope,
      mode: Mode,
      stage: Stage | undefined,
      depth: number,
   ): Src {
      const sub = (x: unknown) => this.expr(x, scope, mode, stage, depth + 1);
      const need = (src: Src, u: Use) => {
         if (mode === "judge") this.consume(src, u);
      };
      const kids = (n.kids ?? {}) as { args?: unknown; orderBy?: unknown };
      const name = String(n.name ?? "").toLowerCase();
      for (const o of asArray(kids.orderBy)) {
         need(sub((o as { e?: unknown }).e), "order");
      }
      for (const p of asArray(n.partitionBy)) {
         const qf =
            stage === undefined ? undefined : stageFields(stage).get(String(p));
         if (qf === undefined) this.unresolved = true;
         else need(this.queryField(qf, scope, "trace", stage!), "group");
      }
      const args = combine(asArray(kids.args).map(sub));
      if (ORDERING_FUNCTIONS.has(name)) need(args, "order");
      else if (EQUALITY_FUNCTIONS.has(name)) need(args, "group");
      else if (COLLATION_FREE_FUNCTIONS.has(name)) return NONE;
      else if (isAggregate(n.expressionType)) {
         if (name.includes("distinct")) need(args, "group");
         else if (!UNORDERED_AGGREGATES.has(name)) need(args, "order");
      }
      const returned = (
         n.overload as { returnType?: { type?: unknown } } | undefined
      )?.returnType?.type;
      if (returned === "string") {
         // A string made from strings has their collation; from none, the
         // default.
         return args.kind === "columns" ? args : DEFAULT;
      }
      if (
         typeof returned === "string" &&
         returned !== "generic" &&
         returned !== "any"
      ) {
         return NONE;
      }
      // A generic return (`greatest`, `nullif`, a window's `lag`) is its
      // arguments' value.
      return args;
   }
}

/** A {@link GateReads} of one class of the walk's reads. */
function readsOf(
   reads: Reads,
   unresolved: boolean,
   equalityOnly: boolean,
): GateReads {
   const keys = [...reads.paths.keys()];
   const entryPaths = keys.map((k) => reads.paths.get(k)!);
   const livePaths = keys
      .filter((k) => reads.live.get(k) === true)
      .map((k) => reads.paths.get(k)!);
   return {
      names: new Set(entryPaths.flat()),
      columns: keys.map((k) => [...(reads.names.get(k) ?? [])]),
      ...(livePaths.length > 0 ? { livePaths } : {}),
      unresolved,
      scoped: false,
      gated: false,
      equalityOnly,
      entryPaths,
      entryPathsComplete: !reads.elsewhere,
      comparesNoString: reads.empty && !unresolved,
      comparesOnlyColumns: !reads.literal,
      ...(reads.same.length > 0 ? { sameCollation: reads.same } : {}),
   };
}

/**
 * What the result of a compiled query (the author's) depends on the order or
 * the equality of strings for, from stage `fromStage` on, or undefined when
 * it cannot be read. `orderOnly` names a stage whose rows a stored result
 * already holds (a persisted view's or named query's own last stage), whose
 * ordering alone is applied again when the table is read. `stored` names
 * (by `sourceID`) the sources whose tables the answer reads.
 */
export async function resultReadsOfQuery(
   runnable: QueryMaterializer,
   stored: ReadonlySet<string>,
   options: {
      fromStage: number;
      orderOnly?: number;
      /**
       * Every source, by ID, whose table the answer reads: `stored`, and a
       * rollup's base. Only {@link ResultReadWalk.knownLive} reads it; absent,
       * `stored`.
       */
      storedSources?: ReadonlySet<string>;
   },
): Promise<ResultReads | undefined> {
   try {
      const prepared = await readPreparedQuery(runnable);
      if (
         !Array.isArray(prepared.pipeline) ||
         prepared.modelDef === undefined
      ) {
         return undefined;
      }
      const named = resolveStructRefSource(
         prepared.structRef as never,
         prepared.modelDef as never,
      ) as { dialect?: unknown } | undefined;
      // A composite reads the member the compile resolved to.
      const resolved = prepared.compositeResolvedSourceDef;
      const entry =
         resolved !== null && typeof resolved === "object" ? resolved : named;
      if (entry === undefined) return undefined;
      const walk = new ResultReadWalk(
         prepared.modelDef,
         stored,
         options.storedSources ?? stored,
      );
      // A query-shaped entry point's own query, which every answer reads.
      walk.inner(entry);
      walk.pipeline(
         prepared.pipeline as Stage[],
         { kind: "source", source: entry, prefix: [], entry: true },
         {
            fromStage: Math.max(0, options.fromStage),
            ...(options.orderOnly !== undefined && options.orderOnly >= 0
               ? { orderOnly: options.orderOnly }
               : {}),
         },
      );
      const dialect = named?.dialect;
      return {
         ordered: readsOf(walk.ordered, walk.unresolved, false),
         grouped: readsOf(walk.grouped, walk.unresolved, true),
         stringCollation: capabilitiesOf(
            typeof dialect === "string" ? dialect : "",
         ).stringCollation,
      };
   } catch {
      return undefined;
   }
}

/**
 * What a chained `storage=` build computes over its stored upstreams depends
 * on the order or the equality of strings for, read with the build's base as
 * the entry point, or undefined when it cannot be read. The build runs on the
 * destination, byte-exact, so each string it groups, compares or orders must
 * compare there as the base's source compares it live.
 *
 * `runnable` is a query on the base (by name). With `downstream`, the build
 * computes that query-shaped source's own query, which must run on the same
 * base; without, the runnable's own pipeline (a rollup's grouping). Either is
 * read as an inner query: its last stage's order is not a table's. The
 * filters inside its stages (a `where:`, a `having:`, a filtered measure's)
 * are the query-filter check's, over the returned `stages`; the base's own
 * `where:`s, and every joined source's and `on`, are read here.
 */
export async function chainBuildReadsOfQuery(
   runnable: QueryMaterializer,
   stored: ReadonlySet<string>,
   downstream?: string,
): Promise<(ResultReads & { stages: readonly unknown[] }) | undefined> {
   try {
      const prepared = await readPreparedQuery(runnable);
      if (prepared.modelDef === undefined) return undefined;
      const entry = resolveStructRefSource(
         prepared.structRef as never,
         prepared.modelDef as never,
      ) as { dialect?: unknown } | undefined;
      if (entry === undefined) return undefined;
      let stages: unknown = prepared.pipeline;
      if (downstream !== undefined) {
         const contents = (
            prepared.modelDef as { contents?: Record<string, unknown> }
         ).contents;
         const query = (
            contents?.[downstream] as
               | { query?: { structRef?: unknown; pipeline?: unknown } }
               | undefined
         )?.query;
         // The downstream's query must run on the runnable's entry point.
         if (
            typeof prepared.structRef !== "string" ||
            baseNameOf(query?.structRef, contents) !== prepared.structRef
         ) {
            return undefined;
         }
         stages = query?.pipeline;
      }
      if (!Array.isArray(stages)) return undefined;
      const walk = new ResultReadWalk(prepared.modelDef, stored, stored);
      const scope: Scope = {
         kind: "source",
         source: entry,
         prefix: [],
         entry: true,
      };
      walk.inner(entry);
      // The base's own `where:`s and its joins', which the build applies
      // again over the stored tables (a stored base's too: its re-root
      // keeps them).
      walk.sourceFilters(entry, []);
      walk.pipeline(stages as Stage[], scope, { fromStage: 0, inner: true });
      return {
         stages,
         ordered: readsOf(walk.ordered, walk.unresolved, false),
         grouped: readsOf(walk.grouped, walk.unresolved, true),
         stringCollation: capabilitiesOf(
            typeof entry.dialect === "string" ? entry.dialect : "",
         ).stringCollation,
      };
   } catch {
      return undefined;
   }
}

/** The base a query-shaped source's own query runs on, by name, or undefined. */
export async function chainBuildBaseOf(
   runnable: QueryMaterializer,
   downstream: string,
): Promise<string | undefined> {
   try {
      const prepared = await readPreparedQuery(runnable);
      const contents = (
         prepared.modelDef as { contents?: Record<string, unknown> } | undefined
      )?.contents;
      return baseNameOf(
         (
            contents?.[downstream] as
               | { query?: { structRef?: unknown } }
               | undefined
         )?.query?.structRef,
         contents,
      );
   } catch {
      return undefined;
   }
}

/**
 * The model's name for the source a query runs on: its reference, or the
 * name declaring the inlined source by its `sourceID`, when exactly one does.
 */
function baseNameOf(
   ref: unknown,
   contents: Record<string, unknown> | undefined,
): string | undefined {
   if (typeof ref === "string") return ref;
   const id = (ref as { sourceID?: unknown } | undefined)?.sourceID;
   if (typeof id !== "string") return undefined;
   const names = Object.entries(contents ?? {})
      .filter(([, d]) => (d as { sourceID?: unknown })?.sourceID === id)
      .map(([name]) => name);
   return names.length === 1 ? names[0] : undefined;
}
