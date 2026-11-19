// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which strings a query's result depends on the order or the equality of,
 * for the check that a `storage=` copy answers as its source does
 * (`gate_collation.ts` judges them, the router asks).
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
 * Whether a function call or a cast makes a string: a cast to a string (a
 * raw cast to a type that may be one), or a function whose declared return
 * type is a string. A generic return (`coalesce`, `greatest`) is its
 * arguments' type, whose own strings are read where they are made.
 */
function makesString(n: Record<string, unknown>): boolean {
   if (n.node === "cast") {
      const dst = (n.dstType as { type?: unknown } | undefined)?.type;
      if (dst !== undefined) return dst === "string";
      return !/^\s*(big|small|tiny)?(int|integer|numeric|decimal|real|double|float|bool|boolean|date|time|timestamp|timestamptz)\b/i.test(
         String(n.dstSQLType ?? ""),
      );
   }
   const returned = (
      n.overload as { returnType?: { type?: unknown } } | undefined
   )?.returnType?.type;
   return returned === "string";
}

/** A path's records, by its key, and whether each is read from the entry point. */
class Reads {
   readonly paths = new Map<string, readonly string[]>();
   /** A read that is not a path from the entry point (an inner query's base). */
   elsewhere = false;
   /**
    * A literal or a given read as a string value (a `pick`'s `'hi'`), which
    * compares under the database's default collation.
    */
   literal = false;

   add(path: readonly string[], entry: boolean): void {
      if (!entry) this.elsewhere = true;
      else this.paths.set(path.join("\u0000"), path);
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
   /** How many string column reads have been recorded, by any use. */
   private recorded = 0;
   /**
    * A string value met so far that compares under the database's default
    * collation (a literal, a given, a string made from no string column),
    * by use: an enclosing derivation that also reads a string column takes
    * that column's collation instead, as Postgres does ({@link derive}).
    */
   pending = { order: false, group: false };
   /** Each join's `on` and each inner query, walked once. */
   private readonly walked = new Set<unknown>();

   constructor(
      private readonly modelDef: unknown,
      private readonly stored: ReadonlySet<string>,
   ) {}

   private tooDeep(): boolean {
      return ++this.steps > 8192;
   }

   private record(path: readonly string[], use: Use, entry: boolean): void {
      if (use === "value") return;
      this.recorded++;
      if (use === "order") this.ordered.add(path, entry);
      else this.grouped.add(path, entry);
   }

   /** A string value under the database's default collation, used as `use`. */
   private loose(use: Use): void {
      if (use === "order") this.pending.order = true;
      else if (use === "group") this.pending.group = true;
   }

   /**
    * One collation derivation (Postgres's): the operands `walk` reads. When
    * one is a string column, the result takes that column's collation, so
    * the default-collation strings met inside (`'!'` in `concat(cat, '!')`,
    * `'M'` in `cat > 'M'`) compare under it, not under the default. When
    * none is, a comparison compares them under the default (committed: no
    * enclosing node changes that), and a function or cast that makes a
    * string (`cast(amt as string)`, `chr(65)`) makes one under the default,
    * which an enclosing derivation may still take a column's collation over.
    */
   private derive(
      kind: "comparison" | "value",
      walk: () => void,
      makesString: boolean,
      use: Use,
   ): void {
      const before = this.recorded;
      const saved = { ...this.pending };
      walk();
      if (this.recorded > before) {
         this.pending = saved;
         return;
      }
      if (kind === "comparison") {
         if (this.pending.order && !saved.order) this.ordered.literal = true;
         if (this.pending.group && !saved.group) this.grouped.literal = true;
         this.pending = saved;
         return;
      }
      if (makesString) this.loose(use);
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
            this.queryField(qf, input, "order", "judge", stage);
         }
         for (const f of asArray(stage.filterList)) {
            this.expr((f as { e?: unknown }).e, input, "value", "judge", stage);
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
         const use: Use =
            stage.type === "reduce" && !isAggregate(kind) && !isAnalytic(kind)
               ? "group"
               : "value";
         this.queryField(qf, input, use, "judge", stage);
      }
      for (const f of asArray(stage.filterList)) {
         this.expr((f as { e?: unknown }).e, input, "value", "judge", stage);
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
         // computed by a stored result's build): only its columns are traced.
         this.queryField(qf, input, "order", "trace", stage);
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

   /** One of a stage's fields, read from the stage's input. */
   private queryField(
      qf: FieldLike,
      input: Scope,
      use: Use,
      mode: Mode,
      stage: Stage,
   ): void {
      if (qf.type === "fieldref" && Array.isArray(qf.path)) {
         this.field(input, qf.path as string[], use, mode);
      } else if (qf.type === "turtle") {
         // A nest's rows, read as a value of a later stage: not followed.
         if (use !== "value") this.unresolved = true;
      } else if (qf.e !== undefined) {
         if (mode === "trace" && nonStringType(qf.type)) return;
         this.expr(qf.e, input, use, mode, stage);
      } else {
         this.unresolved = true;
      }
   }

   /** A field path read in `scope`, to the columns it reads. */
   field(scope: Scope, path: readonly string[], use: Use, mode: Mode): void {
      if (this.tooDeep() || path.length === 0) {
         this.unresolved = true;
         return;
      }
      if (mode === "trace" && use === "value") return;
      if (scope.kind === "stage") {
         const qf =
            path.length === 1
               ? stageFields(scope.stage).get(path[0])
               : undefined;
         if (qf === undefined) {
            if (use !== "value") this.unresolved = true;
            return;
         }
         // A stage's output was computed by that stage, which is judged on
         // its own: only the columns the value comes from are traced.
         this.queryField(qf, scope.input, use, "trace", scope.stage);
         return;
      }
      let owner = scope.source;
      const prefix = [...scope.prefix];
      for (const name of path.slice(0, -1)) {
         const j = fieldsOf(owner).get(name);
         if (j === undefined || j.join === undefined) {
            this.unresolved = true;
            return;
         }
         if (mode === "judge") this.join(owner, j, prefix, scope.entry);
         owner = j;
         prefix.push(name);
      }
      const last = path[path.length - 1];
      const f = fieldsOf(owner).get(last);
      if (f === undefined) {
         this.unresolved = true;
         return;
      }
      const at: Scope = {
         kind: "source",
         source: owner,
         prefix,
         entry: scope.entry,
      };
      if (f.e !== undefined) {
         if (mode === "trace" && nonStringType(f.type)) return;
         this.expr(f.e, at, use, mode, undefined);
         return;
      }
      const query = (owner as { query?: unknown }).query;
      if (
         query !== undefined &&
         f.join === undefined &&
         !this.isStored(owner)
      ) {
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
            return;
         }
         let s: Scope = {
            kind: "source",
            source: base,
            prefix: [],
            entry: false,
         };
         for (const stage of stages) s = { kind: "stage", stage, input: s };
         // A rename keeps the stage output's own name.
         this.field(
            s,
            [typeof f.name === "string" ? f.name : last],
            use,
            "trace",
         );
         return;
      }
      if (f.join !== undefined) {
         // A joined record read as a value: its fields are not compared.
         if (use !== "value") this.unresolved = true;
         return;
      }
      if (!nonStringType(f.type))
         this.record([...prefix, last], use, scope.entry);
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
         this.expr(j.onExpression, at, "value", "judge", undefined);
      }
      const name = activeName(j);
      if (typeof j.primaryKey === "string") {
         this.field(at, [name, j.primaryKey], "group", "trace");
      }
      const own = (owner as { primaryKey?: unknown }).primaryKey;
      if (typeof own === "string") this.field(at, [own], "group", "trace");
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
    * An expression read in `scope` and used as `use`. Judged, each node
    * decides how its operands are used; traced, every operand carries the
    * value's use.
    */
   expr(
      e: unknown,
      scope: Scope,
      use: Use,
      mode: Mode,
      stage: Stage | undefined,
      depth = 0,
   ): void {
      if (depth > 64 || this.tooDeep()) {
         this.unresolved = true;
         return;
      }
      if (e === null || typeof e !== "object" || Array.isArray(e)) return;
      const n = e as Record<string, unknown> & { node?: unknown };
      const go = (x: unknown, u: Use) =>
         this.expr(x, scope, u, mode, stage, depth + 1);
      switch (n.node) {
         case "field":
            if (Array.isArray(n.path)) {
               this.field(scope, n.path as string[], use, mode);
            } else this.unresolved = true;
            return;
         case "outputField": {
            const qf =
               stage === undefined
                  ? undefined
                  : stageFields(stage).get(String(n.name));
            if (qf === undefined) {
               if (use !== "value") this.unresolved = true;
               return;
            }
            this.queryField(qf, scope, use, "trace", stage!);
            return;
         }
         case "compositeField":
         case "error":
         case "spread":
         case "source-reference":
            this.unresolved = true;
            return;
         case "stringLiteral":
         case "given":
            this.loose(use);
            return;
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
            return;
      }
      if (mode === "trace") this.traced(n, use, go);
      else this.judged(n, scope, use, stage, depth, go);
   }

   /** A traced node: every operand carries the value's use. */
   private traced(
      n: Record<string, unknown>,
      use: Use,
      go: (x: unknown, u: Use) => void,
   ): void {
      const kids = (n.kids ?? {}) as Record<string, unknown>;
      const all = () => {
         for (const c of this.children(n)) go(c, use);
      };
      if (n.node === "function_call" || n.node === "cast") {
         this.derive("value", all, makesString(n), use);
      } else if (n.node === "case" && kids.caseValue === undefined) {
         // The value is the `then`s' and the `else`'s; a `when:` was
         // decided when the value was computed.
         this.derive(
            "value",
            () => {
               for (const t of asArray(kids.caseThen)) go(t, use);
               go(kids.caseElse, use);
            },
            false,
            use,
         );
      } else if (n.node === "coalesce") {
         this.derive("value", all, false, use);
      } else all();
   }

   /** A judged node: it decides how its operands are used. */
   private judged(
      n: Record<string, unknown>,
      scope: Scope,
      use: Use,
      stage: Stage | undefined,
      depth: number,
      go: (x: unknown, u: Use) => void,
   ): void {
      const kids = (n.kids ?? {}) as Record<string, unknown>;
      switch (n.node) {
         case "aggregate": {
            const fn = n.function;
            go(
               n.e,
               fn === "min" || fn === "max"
                  ? "order"
                  : fn === "distinct"
                    ? "group"
                    : "value",
            );
            return;
         }
         case "in":
            this.derive(
               "comparison",
               () => {
                  go(kids.e, "group");
                  for (const x of asArray(kids.oneOf)) go(x, "group");
               },
               false,
               use,
            );
            return;
         case "inGiven":
            this.derive("comparison", () => go(n.e, "group"), false, use);
            return;
         case "regexpMatch":
            go(kids.expr, "group");
            go(kids.regex, "value");
            return;
         case "filterMatch":
            go(kids.expr, "group");
            return;
         case "filteredExpr":
            // Its filter is the query-filter check's (`gate_reads.ts`).
            go(kids.e, use);
            return;
         case "case": {
            // A `when:` is a condition of its own; the value is the
            // `then`s' and the `else`'s derivation.
            if (kids.caseValue !== undefined) {
               this.derive(
                  "comparison",
                  () => {
                     go(kids.caseValue, "group");
                     for (const w of asArray(kids.caseWhen)) go(w, "group");
                  },
                  false,
                  use,
               );
            } else {
               for (const w of asArray(kids.caseWhen)) go(w, "value");
            }
            this.derive(
               "value",
               () => {
                  for (const t of asArray(kids.caseThen)) go(t, use);
                  go(kids.caseElse, use);
               },
               false,
               use,
            );
            return;
         }
         case "cast": {
            const dst = (n.dstType as { type?: unknown } | undefined)?.type;
            const toString = makesString(n);
            this.derive(
               "value",
               () =>
                  go(
                     n.e,
                     dst !== undefined && nonStringType(dst) ? "value" : use,
                  ),
               toString,
               use,
            );
            return;
         }
         case "function_call":
            this.functionCall(n, scope, use, stage, depth);
            return;
         case "genericSQLExpr":
            // Raw SQL: what it does with its arguments is not read.
            for (const a of asArray(kids.args)) go(a, "order");
            return;
      }
      if (typeof n.node === "string" && RANGE_OPERATORS.has(n.node)) {
         this.derive(
            "comparison",
            () => {
               for (const c of this.children(n)) go(c, "order");
            },
            false,
            use,
         );
         return;
      }
      if (typeof n.node === "string" && EQUALITY_OPERATORS.has(n.node)) {
         this.derive(
            "comparison",
            () => {
               for (const c of this.children(n)) go(c, "group");
            },
            false,
            use,
         );
         return;
      }
      if (n.node === "coalesce") {
         this.derive(
            "value",
            () => {
               for (const c of this.children(n)) go(c, use);
            },
            false,
            use,
         );
         return;
      }
      // Logic, arithmetic, `()`, time arithmetic, an ungrouping: their
      // operands' values pass on as the value is used.
      for (const c of this.children(n)) go(c, use);
   }

   /** A function call: scalar, aggregate or window. */
   private functionCall(
      n: Record<string, unknown>,
      scope: Scope,
      use: Use,
      stage: Stage | undefined,
      depth: number,
   ): void {
      const go = (x: unknown, u: Use) =>
         this.expr(x, scope, u, "judge", stage, depth + 1);
      const kids = (n.kids ?? {}) as { args?: unknown; orderBy?: unknown };
      const name = String(n.name ?? "").toLowerCase();
      for (const o of asArray(kids.orderBy)) {
         go((o as { e?: unknown }).e, "order");
      }
      for (const p of asArray(n.partitionBy)) {
         const qf =
            stage === undefined ? undefined : stageFields(stage).get(String(p));
         if (qf === undefined) this.unresolved = true;
         else this.queryField(qf, scope, "group", "trace", stage!);
      }
      let argUse: Use = use;
      if (ORDERING_FUNCTIONS.has(name)) argUse = "order";
      else if (EQUALITY_FUNCTIONS.has(name)) argUse = "group";
      else if (COLLATION_FREE_FUNCTIONS.has(name)) argUse = "value";
      else if (isAggregate(n.expressionType)) {
         argUse = name.includes("distinct")
            ? "group"
            : UNORDERED_AGGREGATES.has(name)
              ? "value"
              : "order";
      }
      this.derive(
         "value",
         () => {
            for (const a of asArray(kids.args)) go(a, argUse);
         },
         makesString(n),
         use,
      );
   }
}

/** A {@link GateReads} of one class of the walk's reads. */
function readsOf(
   reads: Reads,
   unresolved: boolean,
   equalityOnly: boolean,
   /** A default-collation string no column's collation took over. */
   pending: boolean,
): GateReads {
   if (pending) reads.literal = true;
   const entryPaths = [...reads.paths.values()];
   return {
      names: new Set(entryPaths.flat()),
      columns: entryPaths.map((p) => [p[p.length - 1]]),
      unresolved,
      scoped: false,
      gated: false,
      equalityOnly,
      entryPaths,
      entryPathsComplete: !reads.elsewhere,
      comparesNoString: reads.empty && !unresolved,
      comparesOnlyColumns: !reads.literal,
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
   options: { fromStage: number; orderOnly?: number },
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
      const walk = new ResultReadWalk(prepared.modelDef, stored);
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
         ordered: readsOf(
            walk.ordered,
            walk.unresolved,
            false,
            walk.pending.order,
         ),
         grouped: readsOf(
            walk.grouped,
            walk.unresolved,
            true,
            walk.pending.group,
         ),
         stringCollation: capabilitiesOf(
            typeof dialect === "string" ? dialect : "",
         ).stringCollation,
      };
   } catch {
      return undefined;
   }
}
