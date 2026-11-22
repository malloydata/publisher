// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What an entry point's row-level gates read from a stored table, for the
 * checks that decide whether a routed answer filters the stored rows as live
 * filters the source's: a query's own time zone (a gate over a stored
 * truncation, `serve/served_copy_checks.ts`, `serve/rollup_variants.ts`) and a
 * gate column's stored type (`domain/gate_policy.ts`).
 *
 * The gate is grafted on the routed compile after the leg answers, so a check
 * that reads only the caller's query never sees the fields the gate reads.
 * Every check that judges stored values against live ones must read these too.
 */

import type {
   ModelDef,
   QueryMaterializer,
   SourceDef,
} from "@malloydata/malloy";
import {
   ACCESS_FILTER_ROUTE,
   canonicalAuthorizeRoute,
} from "../../authorize_routes";
import {
   collectEntryPointGates,
   type GateEntry,
} from "../../gate_classification";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import { stripMalloyCommentsAndLiterals } from "../../malloy_lexer_core";
import { malloyIdentifiers } from "../../query_text";

import { fieldPathsIn } from "./gate_paths";
import type { ColumnOrigin, GateReads } from "./gate_reads_types";
import type { UpstreamTableSchema } from "../domain/upstream_schemas";

/** The names a gate's text reads: each identifier it names, fields or not. */
export function gateIdentifiers(exprs: readonly string[]): string[] {
   return [...new Set(exprs.flatMap((expr) => malloyIdentifiers(expr)))];
}

/** Words a gate's text may hold that name no field. */
const GATE_KEYWORDS = new Set([
   "and",
   "or",
   "not",
   "in",
   "is",
   "true",
   "false",
   "null",
]);

/**
 * The field paths a gate's text reads (`u.acct = $ACCT` reads `u.acct`), its
 * givens, literals and keywords left out; `unresolved` when the text holds a
 * form this reader does not follow (a quoted name).
 */
function gatePaths(exprs: readonly string[]): {
   paths: string[][];
   unresolved: boolean;
} {
   const paths: string[][] = [];
   let unresolved = false;
   for (const expr of exprs) {
      if (expr.includes("`")) unresolved = true;
      const text = stripMalloyCommentsAndLiterals(expr).replace(
         /\$[A-Za-z_][A-Za-z_0-9]*/g,
         " ",
      );
      for (const m of text.matchAll(
         /(?<![\w$.])[A-Za-z_][A-Za-z_0-9]*(?:\.[A-Za-z_][A-Za-z_0-9]*)*/g,
      )) {
         const rest = text.slice((m.index ?? 0) + m[0].length);
         // A function call names no field.
         if (/^\s*\(/.test(rest)) continue;
         const path = m[0].split(".");
         if (path.length === 1 && GATE_KEYWORDS.has(path[0].toLowerCase())) {
            continue;
         }
         paths.push(path);
      }
   }
   return { paths, unresolved };
}

export type { GateReads } from "./gate_reads_types";

/**
 * The operators a comparison of non-string values may be built with. Time
 * arithmetic is among them: `delta` (`now - 3650 days`, its `base` and its
 * count both operands), `trunc` (`created_at.month`) and `extract`
 * (`day(now)`) each make a time or a number of their operands alone, so they
 * compare a string only when an operand is one, which the walk still reads.
 * So do `timeDiff` (`hours(ts to now)`) and arithmetic (`+ - * / %`, a unary
 * minus), whose operands Malloy types as numbers or times.
 */
const NON_STRING_OPERATORS = new Set([
   "=",
   "!=",
   "<",
   "<=",
   ">",
   ">=",
   "in",
   "not",
   "and",
   "or",
   "()",
   "is-null",
   "is-not-null",
   "delta",
   "trunc",
   "extract",
   "timeDiff",
   "+",
   "-",
   "*",
   "/",
   "%",
   "unary-",
]);

/** A known type that is not a string. Unknown is not. */
function nonStringType(t: unknown): boolean {
   return typeof t === "string" && t !== "string";
}

/** The type of the field a path names on `source`, through joins. */
function typeAt(source: unknown, path: readonly string[]): unknown {
   return fieldAt(source, path)?.type;
}

/** The IR nodes an equality-only comparison is built from. */
const EQUALITY_NODES = new Set([
   "=",
   "!=",
   "in",
   "not",
   "and",
   "or",
   "()",
   "field",
   "given",
   "stringLiteral",
   "numberLiteral",
   "true",
   "false",
   "null",
   "is-null",
   "is-not-null",
]);

/**
 * Whether an expression compares only by equality: every IR node it holds is
 * one of {@link EQUALITY_NODES}. Any other node, known or not, reads false.
 */
export function equalityOnlyExpr(e: unknown, depth = 0): boolean {
   if (depth > 64) return false;
   if (e === null || typeof e !== "object") return true;
   if (Array.isArray(e)) return e.every((x) => equalityOnlyExpr(x, depth + 1));
   const n = (e as { node?: unknown }).node;
   if (typeof n === "string" && !EQUALITY_NODES.has(n)) return false;
   return Object.values(e).every((v) => equalityOnlyExpr(v, depth + 1));
}

interface FieldLike {
   name?: unknown;
   as?: unknown;
   e?: unknown;
   join?: unknown;
   type?: unknown;
   path?: unknown;
   fields?: unknown;
   expressionType?: unknown;
}

function activeName(f: FieldLike): string {
   return String(f.as ?? f.name);
}

function fieldsOf(struct: unknown): Map<string, FieldLike> {
   const out = new Map<string, FieldLike>();
   for (const f of ((struct as { fields?: unknown[] } | undefined)?.fields ??
      []) as FieldLike[]) {
      out.set(activeName(f), f);
   }
   return out;
}

/** One stage of a compiled query, as the query-filter walk reads it. */
interface QueryStage {
   filterList?: unknown;
   queryFields?: unknown[];
   outputStruct?: unknown;
   orderBy?: unknown;
}

/** What the query-filter walk carries through one query. */
interface FilterContext {
   readonly entry: unknown;
   readonly pipeline: readonly QueryStage[];
   readonly namedMeasures: boolean;
   /** The named measures already followed, by path. */
   readonly followed: Set<string>;
   /**
    * The first stage applied to stored rows, and what the stage before it
    * outputs (the entry point when 0): a stage before it was computed by
    * the build, so its outputs are stored columns, read by their type.
    */
   readonly fromStage: number;
   readonly base: unknown;
}

/** The path `e` names when it is only a field reference, else undefined. */
function bareFieldPath(e: unknown): string[] | undefined {
   const n = e as { node?: unknown; path?: unknown } | undefined;
   return n?.node === "field" &&
      Array.isArray(n.path) &&
      n.path.length > 0 &&
      n.path.every((p) => typeof p === "string")
      ? (n.path as string[])
      : undefined;
}

/** Whether an expression type is a window function's. */
function isAnalyticKind(t: unknown): boolean {
   return typeof t === "string" && t.includes("analytic");
}

/** The input of a stage after the first: an earlier stage's output. */
const LATER_STAGE = Symbol("later stage");

function asArray(x: unknown): unknown[] {
   return Array.isArray(x) ? x : [];
}

/** The field a path names on `source`, through joins, or undefined. */
function fieldAt(
   source: unknown,
   path: readonly string[],
): (FieldLike & { expressionType?: unknown }) | undefined {
   let at: unknown = source;
   let field: FieldLike | undefined;
   for (const name of path) {
      field = fieldsOf(at).get(name);
      if (field === undefined) return undefined;
      at = field;
   }
   return field;
}

/**
 * The terms of every filtered expression (`count() { where: … }`) in `e`,
 * at any depth.
 */
function filteredTerms(e: unknown, out: unknown[] = [], depth = 0): unknown[] {
   if (depth > 64 || e === null || typeof e !== "object") return out;
   if (Array.isArray(e)) {
      for (const x of e) filteredTerms(x, out, depth + 1);
      return out;
   }
   const n = e as { node?: unknown; kids?: { filterList?: unknown } };
   if (n.node === "filteredExpr") {
      for (const f of asArray(n.kids?.filterList)) {
         out.push((f as { e?: unknown }).e);
      }
   }
   for (const v of Object.values(e)) filteredTerms(v, out, depth + 1);
   return out;
}

/**
 * `e` with each field reference (`node: "field"`) replaced by what `to`
 * returns for its path, or undefined when `to` returns undefined for any.
 */
function withFields(
   e: unknown,
   to: (path: string[]) => unknown,
   depth = 0,
): unknown {
   if (depth > 64) return undefined;
   if (e === null || typeof e !== "object") return e;
   if (Array.isArray(e)) {
      const out = e.map((x) => withFields(x, to, depth + 1));
      return out.some((x, i) => x === undefined && e[i] !== undefined)
         ? undefined
         : out;
   }
   const n = e as { node?: unknown; path?: unknown };
   if (n.node === "field" && Array.isArray(n.path)) {
      return to(n.path as string[]);
   }
   const out: Record<string, unknown> = {};
   for (const [k, v] of Object.entries(e)) {
      const w = withFields(v, to, depth + 1);
      if (w === undefined && v !== undefined) return undefined;
      out[k] = w;
   }
   return out;
}

/** A query-shaped source's query, as the walk reads it. */
interface QueryLike {
   structRef?: unknown;
   pipeline?: unknown;
}

/**
 * The warehouse column a plain field named `column` of `source` reads, when
 * `source` is a `.table()` source; undefined for any other relation (a
 * query stage, author SQL), whose column the build computed.
 */
function tableColumnOf(
   source: unknown,
   column: string,
): ColumnOrigin | undefined {
   const s = source as
      | { type?: unknown; tablePath?: unknown; connection?: unknown }
      | undefined;
   return s?.type === "table" &&
      typeof s.tablePath === "string" &&
      typeof s.connection === "string"
      ? { connectionName: s.connection, table: s.tablePath, column }
      : undefined;
}

/** One stage's output field still to follow back, and the names it went by. */
interface Wanted {
   path: string[];
   chain: string[];
}

/**
 * The walk behind {@link gateReads}: each name a gate or a row scope reads,
 * followed to the columns it reaches, recording what could not be followed.
 */
class GateReadWalk {
   readonly names = new Set<string>();
   readonly columns: string[][] = [];
   /** Each of {@link columns}'s origin, by position (GateReads.columnOrigins). */
   readonly columnOrigins: (ColumnOrigin | undefined)[] = [];
   unresolved = false;
   scoped = false;
   equalityOnly = true;
   comparesNoString = true;
   /**
    * Every string comparison read so far compares a plain column against
    * values that take no collation of their own (`GateReads.comparesOnlyColumns`).
    */
   comparesOnlyColumns = true;
   readonly entryPaths: string[][] = [];
   entryPathsComplete = true;
   private steps = 0;

   constructor(
      private readonly modelDef: unknown,
      private readonly stored: ReadonlySet<string>,
      /**
       * The sources the answer's compile re-rooted on their stored tables,
       * by `sourceID`, read or not ({@link scopeJoins}).
       */
      private readonly rerooted: ReadonlySet<string> = new Set(),
   ) {}

   /** A string comparison not known to compare only columns. */
   private noColumnsOnly(): void {
      this.comparesNoString = false;
      this.comparesOnlyColumns = false;
   }

   /**
    * Whether every comparison in `e` (a query's own filter term on
    * `owner`'s fields) compares under a column's collation, as Postgres
    * derives it: a plain string column (no expression, not a stage output)
    * against string literals or givens, whose collation the column's
    * outranks, or values that are not strings at all, under `and`, `or`,
    * `not`. Two columns, a function, a computed field or any other node
    * reads false, and the comparison is judged under the default too.
    */
   private onlyColumnComparisons(
      e: unknown,
      owner: unknown,
      depth = 0,
   ): boolean {
      if (depth > 64 || e === null || typeof e !== "object") return false;
      const n = e as {
         node?: unknown;
         e?: unknown;
         kids?: {
            left?: unknown;
            right?: unknown;
            e?: unknown;
            oneOf?: unknown;
         };
      };
      const rec = (x: unknown) =>
         this.onlyColumnComparisons(x, owner, depth + 1);
      const coercible = (x: unknown) => {
         const k = (x as { node?: unknown } | null)?.node;
         return k === "stringLiteral" || k === "given" || k === "null";
      };
      const notString = (x: unknown) => {
         const k = (x as { node?: unknown; path?: unknown } | null)?.node;
         if (k === "field") {
            const t = typeAt(
               owner,
               asArray((x as { path?: unknown }).path) as string[],
            );
            return nonStringType(t);
         }
         return (
            k === "numberLiteral" ||
            k === "true" ||
            k === "false" ||
            k === "null" ||
            k === "dateLiteral" ||
            k === "timestampLiteral"
         );
      };
      const column = (x: unknown) => this.plainStringColumn(x, owner);
      switch (n.node) {
         case "and":
         case "or":
            return rec(n.kids?.left) && rec(n.kids?.right);
         case "not":
         case "()":
            return rec(n.e);
         case "is-null":
         case "is-not-null":
            return true;
         case "=":
         case "!=":
         case ">":
         case "<":
         case ">=":
         case "<=":
         case "like":
         case "!like": {
            const { left, right } = n.kids ?? {};
            if (notString(left) && notString(right)) return true;
            return (
               (column(left) && coercible(right)) ||
               (coercible(left) && column(right))
            );
         }
         case "in": {
            const oneOf = n.kids?.oneOf;
            return (
               column(n.kids?.e) &&
               Array.isArray(oneOf) &&
               oneOf.every(coercible)
            );
         }
         default:
            return false;
      }
   }

   /** Whether `x` reads a stored string column as it is, on `owner`. */
   private plainStringColumn(x: unknown, owner: unknown): boolean {
      const f = x as { node?: unknown; path?: unknown } | null;
      if (
         f?.node !== "field" ||
         !Array.isArray(f.path) ||
         f.path.length === 0
      ) {
         return false;
      }
      let at: unknown = owner;
      let field: FieldLike | undefined;
      for (const name of f.path as string[]) {
         // A query-shaped source's field is a stage output, computed.
         if ((at as { query?: unknown } | undefined)?.query !== undefined) {
            return false;
         }
         field = fieldsOf(at).get(name);
         if (field === undefined) return false;
         at = field;
      }
      return (
         field?.type === "string" &&
         field.e === undefined &&
         field.join === undefined
      );
   }

   /** A given's declared type, by its id in the model, or undefined. */
   givenType(id: unknown): string | undefined {
      const g = (
         this.modelDef as {
            givens?: Record<string, { type?: { type?: unknown } }>;
         }
      )?.givens?.[String(id)];
      const t = g?.type?.type;
      return typeof t === "string" ? t : undefined;
   }

   /** The declared types of the givens named `name`, every one. */
   givenTypesNamed(name: string): (string | undefined)[] {
      const givens = (
         this.modelDef as {
            givens?: Record<
               string,
               { name?: unknown; type?: { type?: unknown } }
            >;
         }
      )?.givens;
      return Object.values(givens ?? {})
         .filter((g) => g.name === name)
         .map((g) =>
            typeof g.type?.type === "string" ? g.type.type : undefined,
         );
   }

   /**
    * Whether every leaf of `e` is a non-string value ({@link GateReads}'s
    * `comparesNoString`), its fields typed on `source`.
    */
   leavesNotString(
      e: unknown,
      source: unknown,
      depth = 0,
      /** Judge the joins a field is read through ({@link joinsCompareNoString}). */
      throughJoins = true,
   ): boolean {
      if (depth > 64 || e === null || typeof e !== "object") return false;
      const n = e as { node?: unknown; path?: unknown; id?: unknown };
      switch (n.node) {
         case "field":
            return (
               Array.isArray(n.path) &&
               nonStringType(typeAt(source, n.path as string[])) &&
               (!throughJoins ||
                  this.joinsCompareNoString(source, n.path as string[]))
            );
         case "given":
            return nonStringType(this.givenType(n.id));
         case "numberLiteral":
         case "true":
         case "false":
         case "null":
         case "dateLiteral":
         case "timestampLiteral":
         case "timestamptzLiteral":
         case "now":
            return true;
         case "case":
            return this.pickLeavesNotString(e, source, depth, throughJoins);
         case "is-null":
         case "is-not-null":
            // A null test of a field compares no value: a stored copy holds
            // its nulls where live does, under any collation, as long as
            // the joins it is read through match rows comparing no string.
            // Of anything else (`nullif(name, 'x') is null`), its operand is
            // judged. A computed field is judged where it is followed.
            {
               const path = bareFieldPath((e as { e?: unknown }).e);
               if (
                  path !== undefined &&
                  (!throughJoins || this.joinsCompareNoString(source, path))
               ) {
                  return true;
               }
            }
            break;
         default:
            if (
               typeof n.node !== "string" ||
               !NON_STRING_OPERATORS.has(n.node)
            ) {
               return false;
            }
      }
      for (const [k, v] of Object.entries(e)) {
         if (k === "node" || k === "at" || k === "typeDef") continue;
         if (v === null || typeof v !== "object") continue;
         const kids = Array.isArray(v) ? v : [v];
         for (const kid of kids) {
            // `kids: { left, right }` holds the operands one level down.
            const operands =
               kid !== null &&
               typeof kid === "object" &&
               !("node" in (kid as object))
                  ? Object.values(kid as object)
                  : [kid];
            for (const o of operands) {
               if (!this.leavesNotString(o, source, depth + 1, throughJoins)) {
                  return false;
               }
            }
         }
      }
      return true;
   }

   /**
    * Whether a `pick` (`node: "case"`) has only non-string leaves: it
    * compares only what its conditions (and a `pick … when` value) compare,
    * and yields only what its branches yield, so `pick 1 when amt > 1 else
    * 0` compares no string, while a condition or a branch holding one
    * (`pick 1 when category > 'M' else 0`, `pick 'a' when … else 'b'`) does.
    * A shape other than one or more conditions, each with its branch, and
    * an optional value and `else`, reads false.
    */
   private pickLeavesNotString(
      e: unknown,
      source: unknown,
      depth: number,
      throughJoins: boolean,
   ): boolean {
      const kids = (e as { kids?: unknown }).kids;
      if (kids === null || typeof kids !== "object") return false;
      const { caseValue, caseWhen, caseThen, caseElse, ...rest } = kids as {
         caseValue?: unknown;
         caseWhen?: unknown;
         caseThen?: unknown;
         caseElse?: unknown;
      };
      if (
         Object.keys(rest).length > 0 ||
         !Array.isArray(caseWhen) ||
         !Array.isArray(caseThen) ||
         caseWhen.length === 0 ||
         caseWhen.length !== caseThen.length
      ) {
         return false;
      }
      return [caseValue, ...caseWhen, ...caseThen, caseElse]
         .filter((x) => x !== undefined)
         .every((x) =>
            this.leavesNotString(x, source, depth + 1, throughJoins),
         );
   }

   /**
    * Whether each join `path` is read through (all but its last name)
    * matches its rows comparing no string: whether a joined row is there at
    * all, so whether its fields are null, follows its `on:`, which a
    * collation decides when it compares strings (`on: cat = tt.category`).
    * Its `on:` is read on the source that declares the join, its leaves by
    * type alone.
    */
   joinsCompareNoString(source: unknown, path: readonly string[]): boolean {
      let at = source;
      for (const name of path.slice(0, -1)) {
         const f = fieldsOf(at).get(name);
         if (f === undefined) return false;
         if (f.join !== undefined) {
            const on = (f as { onExpression?: unknown }).onExpression;
            if (on !== undefined && !this.leavesNotString(on, at, 0, false)) {
               return false;
            }
         }
         at = f;
      }
      return true;
   }

   /**
    * Whether `e` keeps {@link GateReads}'s `equalityOnly`: an equality, or a
    * comparison whose every leaf is a non-string value, which no collation
    * orders (`amount > 100`).
    */
   private equalityOrNotString(e: unknown, source: unknown): boolean {
      return equalityOnlyExpr(e) || this.leavesNotString(e, source);
   }

   /**
    * Whether the answer reads `source`'s own table: its `sourceID` is
    * stored, or it extends a stored query-shaped source and carries that
    * source's query unchanged (`p_plain extend { where: … }`), whose stage
    * outputs are then that table's columns as well.
    */
   private isStored(source: unknown): boolean {
      const s = source as { sourceID?: unknown; query?: unknown } | undefined;
      if (typeof s?.sourceID === "string" && this.stored.has(s.sourceID)) {
         return true;
      }
      if (s?.query === undefined) return false;
      const query = JSON.stringify(s.query);
      return this.storedQueries().has(query);
   }

   /** The queries of the stored query-shaped sources the model names. */
   private storedQueriesMemo?: Set<string>;
   private storedQueries(): Set<string> {
      if (this.storedQueriesMemo) return this.storedQueriesMemo;
      const out = new Set<string>();
      const contents =
         (this.modelDef as { contents?: Record<string, unknown> } | undefined)
            ?.contents ?? {};
      for (const c of Object.values(contents)) {
         const d = c as { sourceID?: unknown; query?: unknown } | undefined;
         if (
            typeof d?.sourceID === "string" &&
            this.stored.has(d.sourceID) &&
            d.query !== undefined
         ) {
            out.add(JSON.stringify(d.query));
         }
      }
      this.storedQueriesMemo = out;
      return out;
   }

   private tooDeep(): boolean {
      return ++this.steps > 4096;
   }

   /** The source a query-shaped source's query runs on, or undefined. */
   private baseOf(query: QueryLike): unknown {
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

   /**
    * Follow a field path of `source` to the columns it reads. `entry`: the
    * path, behind `joins`, is read from the entry point, so a computed
    * field's reads are entry paths too.
    */
   resolve(
      source: unknown,
      path: readonly string[],
      chain: readonly string[],
      joins: readonly string[] = [],
      entry = false,
   ): void {
      if (this.tooDeep() || path.length === 0) {
         this.unresolved = true;
         return;
      }
      const f = fieldsOf(source).get(path[0]);
      this.names.add(path[0]);
      if (f === undefined) {
         this.unresolved = true;
         return;
      }
      if (path.length > 1) {
         if (f.join === undefined) {
            this.unresolved = true;
            return;
         }
         // A stored table holds a joined source's fields under the join's
         // name: an inlined join's record, or flattened as `<join>__<field>`.
         this.resolve(
            f,
            path.slice(1),
            [...chain, path[0]],
            [...joins, path[0]],
            entry,
         );
         return;
      }
      this.field(source, f, chain, joins, entry);
   }

   /** Follow one field: a rename, an expression, a stage output or a column. */
   private field(
      source: unknown,
      f: FieldLike,
      chain: readonly string[],
      joins: readonly string[],
      entry: boolean,
   ): void {
      const name = activeName(f);
      const stored = typeof f.name === "string" ? f.name : name;
      this.names.add(name);
      this.names.add(stored);
      const along = [...chain, name, stored];
      if (f.e !== undefined) {
         if (!this.equalityOrNotString(f.e, source)) this.equalityOnly = false;
         // A computed field's own comparison is the gate's too: a boolean
         // over a string (`late is region > 'M'`) orders that string.
         if (!this.leavesNotString(f.e, source)) this.noColumnsOnly();
         const reads = fieldPathsIn(f.e);
         // An expression that names no field it reads (a composite's field,
         // resolved per member at compile) has no path a probe could read:
         // which member a query resolves to depends on the whole query.
         if (reads.length === 0) this.entryPathsComplete = false;
         for (const p of reads) {
            // The columns a computed field reads are what its comparison
            // compares: a probe reads their collation by these paths.
            if (entry) this.entryPaths.push([...joins, ...p]);
            else this.entryPathsComplete = false;
            this.resolve(source, p, along, joins, entry);
         }
         return;
      }
      if (f.join !== undefined) {
         // A gate comparing a join itself reads no column it can name.
         this.unresolved = true;
         return;
      }
      // A stage output of a source whose own table the answer reads is a
      // column of that table: its build evaluated the stage, in the source's
      // own warehouse as live does, so the read compares only the stored
      // column, as for any other column. A stage computed when the table is
      // read (a query-shaped source over a stored one) is followed back.
      if (
         (source as { query?: unknown } | undefined)?.query !== undefined &&
         !this.isStored(source)
      ) {
         this.throughStages(source, stored, along, joins);
         return;
      }
      const flattened =
         joins.length === 0 ? [] : along.map((n) => [...joins, n].join("__"));
      this.columns.push([...new Set([...along, ...flattened])]);
      this.columnOrigins.push(tableColumnOf(source, stored));
   }

   /** The fields one stage outputs, by the name each goes by. */
   private stageFields(stage: {
      queryFields?: unknown[];
   }): Map<string, FieldLike> {
      const byName = new Map<string, FieldLike>();
      for (const qf of (stage.queryFields ?? []) as FieldLike[]) {
         if (qf.type === "fieldref" && Array.isArray(qf.path)) {
            const p = qf.path as string[];
            byName.set(p[p.length - 1], qf);
         } else if (qf.name !== undefined) {
            byName.set(activeName(qf), qf);
         }
      }
      return byName;
   }

   /**
    * One stage's step back: what each wanted output reads in its input.
    * `own`: the stage is the caller's own query's, so a window function's
    * output is read through what orders and partitions it, and a rename
    * (`value is category`) as the field it names.
    */
   private stepBack(
      byName: Map<string, FieldLike>,
      wanted: Wanted[],
      own?: QueryStage,
   ): Wanted[] {
      const next: Wanted[] = [];
      for (const w of wanted) {
         if (w.path.length !== 1 || this.tooDeep()) {
            this.unresolved = true;
            continue;
         }
         const qf = byName.get(w.path[0]);
         this.names.add(w.path[0]);
         const chain = [...w.chain, w.path[0]];
         if (qf === undefined) {
            this.unresolved = true;
         } else if (qf.type === "fieldref" && Array.isArray(qf.path)) {
            next.push({ path: qf.path as string[], chain });
         } else if (own !== undefined && bareFieldPath(qf.e) !== undefined) {
            next.push({ path: bareFieldPath(qf.e)!, chain });
         } else if (own !== undefined && isAnalyticKind(qf.expressionType)) {
            // Its value follows the order of the strings it is ordered and
            // partitioned by (`row_number()` over `order_by: value`), so it
            // compares them as an ordering does.
            this.equalityOnly = false;
            this.noColumnsOnly();
            const read = this.windowReads(qf, own, byName);
            if (read === undefined) this.unresolved = true;
            else for (const p of read) next.push({ path: p, chain });
         } else if (qf.e !== undefined) {
            if (!equalityOnlyExpr(qf.e)) this.equalityOnly = false;
            // A stage's expression reads the stage's input, not typed here,
            // and has no path from the entry point.
            if (!this.leavesNotString(qf.e, undefined)) {
               this.noColumnsOnly();
            }
            this.entryPathsComplete = false;
            const read = fieldPathsIn(qf.e);
            if (read.length === 0) this.unresolved = true;
            for (const p of read) next.push({ path: p, chain });
         } else {
            this.unresolved = true;
         }
      }
      return next;
   }

   /**
    * What a window function's output (`scalar_analytic`) reads in its
    * stage's input: the fields its arguments name, and through the stage's
    * own outputs, what it is partitioned and ordered by (its own `order_by:`
    * and the stage's, which a window without one follows). Undefined when it
    * reads anything else: an aggregate, another window function, an output
    * the stage does not hold.
    */
   private windowReads(
      qf: FieldLike,
      stage: QueryStage,
      byName: Map<string, FieldLike>,
   ): string[][] | undefined {
      if (qf.expressionType !== "scalar_analytic") return undefined;
      const paths: string[][] = [];
      const outputs = new Set<string>();
      let readable = true;
      const visit = (x: unknown, depth: number): void => {
         if (depth > 64) readable = false;
         if (!readable || x === null || typeof x !== "object") return;
         if (Array.isArray(x)) {
            for (const y of x) visit(y, depth + 1);
            return;
         }
         const n = x as {
            node?: unknown;
            path?: unknown;
            name?: unknown;
            expressionType?: unknown;
            partitionBy?: unknown;
         };
         if (n.node === "aggregate" || n.expressionType === "aggregate") {
            readable = false;
            return;
         }
         if (n.node === "field" && Array.isArray(n.path)) {
            paths.push(n.path as string[]);
         } else if (n.node === "outputField" && typeof n.name === "string") {
            outputs.add(n.name);
         }
         for (const p of asArray(n.partitionBy)) outputs.add(String(p));
         for (const [k, v] of Object.entries(x)) {
            // A function's overload holds its SQL per dialect, no field.
            if (k !== "overload") visit(v, depth + 1);
         }
      };
      visit(qf.e, 0);
      const order = asArray(stage.queryFields).map((f) => {
         const g = f as FieldLike;
         return g.type === "fieldref" && Array.isArray(g.path)
            ? String((g.path as unknown[])[(g.path as unknown[]).length - 1])
            : activeName(g);
      });
      for (const o of asArray(stage.orderBy)) {
         const key = (o as { field?: unknown }).field;
         const name = typeof key === "number" ? order[key - 1] : key;
         if (typeof name !== "string") return undefined;
         outputs.add(name);
      }
      if (!readable) return undefined;
      for (const name of outputs) {
         const out = byName.get(name);
         this.names.add(name);
         if (out === undefined || out === qf) return undefined;
         if (out.type === "fieldref" && Array.isArray(out.path)) {
            paths.push(out.path as string[]);
            continue;
         }
         const kind = out.expressionType ?? "scalar";
         if (out.e === undefined || kind !== "scalar") return undefined;
         const bare = bareFieldPath(out.e);
         if (bare !== undefined) {
            paths.push(bare);
            continue;
         }
         // A computed string has no path from the entry point.
         this.entryPathsComplete = false;
         paths.push(...fieldPathsIn(out.e));
      }
      return paths;
   }

   /** Follow an output field of a query-shaped source back to its base. */
   private throughStages(
      source: unknown,
      output: string,
      chain: readonly string[],
      joins: readonly string[],
      /** The stage whose output `output` names; the last one by default. */
      fromStage?: number,
      /**
       * The query is the caller's own, run on the entry point: what its first
       * stage reads is an entry path.
       */
      entry = false,
   ): void {
      const query = (source as { query: QueryLike }).query;
      const stages = (Array.isArray(query.pipeline) ? query.pipeline : []) as {
         queryFields?: unknown[];
      }[];
      if (stages.length === 0) {
         this.unresolved = true;
         return;
      }
      let wanted: Wanted[] = [{ path: [output], chain: [...chain] }];
      for (let i = fromStage ?? stages.length - 1; i >= 0; i--) {
         wanted = this.stepBack(
            this.stageFields(stages[i]),
            wanted,
            entry ? (stages[i] as QueryStage) : undefined,
         );
         // An earlier stage's output is one name; only the first stage reads
         // the source, through a join path or a name.
         if (i > 0 && wanted.some((n) => n.path.length !== 1)) {
            this.unresolved = true;
         }
      }
      const base = this.baseOf(query);
      if (base === undefined) {
         this.unresolved = true;
         return;
      }
      for (const w of wanted) {
         if (entry) this.entryPaths.push([...joins, ...w.path]);
         this.resolve(base, w.path, w.chain, joins, entry);
      }
   }

   /**
    * The `where:` terms that scope the rows read from `source`, each field
    * path read by `at` (which resolves it from where the walk started,
    * through the joins that reached `source`). At a source whose table the
    * answer reads, its own query (the base it runs on, and each stage's
    * `where:`) is not walked: the build applied it before the table was
    * written. Everything applied when the table is read is still judged: the
    * source's own `where:` (a term that reads a given is never in the build,
    * and a `#@ persist` source's never is), its joins' own `where:`, and its
    * composite members'. A source that extends it judges what it inherits.
    */
   scope(
      source: unknown,
      depth: number,
      at: (path: readonly string[]) => void,
      /** `at` resolves from `source` itself, through no join. */
      own: boolean,
      /** `source` is joined, and the answer's compile re-rooted it on its table. */
      onTable = false,
   ): void {
      if (depth > 32 || source === null || typeof source !== "object") return;
      const s = source as {
         sourceID?: unknown;
         filterList?: unknown;
         query?: QueryLike;
      };
      for (const f of Array.isArray(s.filterList) ? s.filterList : []) {
         this.scoped = true;
         const e = (f as { e?: unknown }).e;
         if (!this.equalityOrNotString(e, source)) this.equalityOnly = false;
         if (!this.leavesNotString(e, source)) this.noColumnsOnly();
         for (const p of fieldPathsIn(e)) {
            if (depth === 0) this.entryPaths.push(p);
            else this.entryPathsComplete = false;
            at(p);
         }
      }
      this.scopeMembers(source, depth, at, own);
      this.scopeJoins(source, depth, at);
      if (s.query !== undefined && !onTable && !this.isStored(source)) {
         this.scopeQuery(source, s.query, depth);
      }
   }

   /** A composite's members: each one's terms, on its own fields. */
   private scopeMembers(
      source: unknown,
      depth: number,
      at: (path: readonly string[]) => void,
      own: boolean,
   ): void {
      const s = source as { type?: unknown; sources?: unknown };
      if (s.type !== "composite" || !Array.isArray(s.sources)) return;
      // A member's terms read the member's own fields; reached through a
      // join, they are read through that join's path.
      for (const m of s.sources) {
         this.scope(
            m,
            depth + 1,
            own ? (p) => this.resolve(m, p, []) : at,
            own,
         );
      }
   }

   /**
    * Each joined source's own `where:`, which filters the rows the join
    * reaches. A joined source the answer's compile re-rooted on its table is
    * read, if at all, from that table, whose build applied its query, as an
    * answer that reads it already holds ({@link isStored}); one the answer
    * does not read is in no SQL it runs. Either way its own query is not
    * walked; what is applied on read (its `where:`, its joins') still is.
    */
   private scopeJoins(
      source: unknown,
      depth: number,
      at: (path: readonly string[]) => void,
   ): void {
      const fields = (source as { fields?: unknown }).fields;
      for (const f of (Array.isArray(fields) ? fields : []) as FieldLike[]) {
         if (f.join === undefined) continue;
         const name = activeName(f);
         const id = (f as { sourceID?: unknown }).sourceID;
         const onTable = typeof id === "string" && this.rerooted.has(id);
         this.scope(f, depth + 1, (p) => at([name, ...p]), false, onTable);
      }
   }

   /**
    * A query-shaped source's base's terms and each stage's own (its `where:`,
    * `having:` and each filtered measure's `{ where: … }`), a later stage's
    * read back through the earlier ones.
    */
   private scopeQuery(source: unknown, query: QueryLike, depth: number): void {
      const base = this.baseOf(query);
      const atBase = (p: readonly string[]) => {
         if (base === undefined) this.unresolved = true;
         else this.resolve(base, p, []);
      };
      if (base === undefined) this.unresolved = true;
      else this.scope(base, depth + 1, atBase, true);
      const stages = Array.isArray(query.pipeline) ? query.pipeline : [];
      stages.forEach((stage, i) => {
         const { filterList, queryFields } = stage as {
            filterList?: unknown;
            queryFields?: unknown;
         };
         const terms = [
            ...asArray(filterList).map((f) => (f as { e?: unknown }).e),
            ...asArray(queryFields).flatMap((qf) =>
               filteredTerms((qf as { e?: unknown }).e),
            ),
         ];
         for (const e of terms) {
            this.scoped = true;
            if (!equalityOnlyExpr(e)) this.equalityOnly = false;
            this.entryPathsComplete = false;
            // A stage's term reads the stage's fields, not typed here.
            this.noColumnsOnly();
            for (const p of fieldPathsIn(e)) {
               if (i === 0) atBase(p);
               else if (p.length === 1) {
                  this.throughStages(source, p[0], [], [], i - 1);
               } else this.unresolved = true;
            }
         }
      });
   }

   /**
    * The string comparisons the caller's own query makes on the rows it
    * reads (`where:`, `having:`, a filtered measure's `{ where: … }`, a
    * view's or a nest's filter), on every stage from `fromStage` on: what an
    * answer re-applies to a stored table, as the scope is. A term that
    * compares no string is not followed (a stored collation cannot change
    * it); every other term's fields are, as a scope's are, its first-stage
    * reads as entry paths. `namedMeasures`: a measure the model names is
    * evaluated when the table is read, so its filter is followed too (not on
    * a rollup, whose build applied each measure it stores).
    */
   queryFilters(
      entry: unknown,
      pipeline: readonly unknown[],
      fromStage: number,
      namedMeasures: boolean,
   ): void {
      const stages = pipeline as QueryStage[];
      const ctx: FilterContext = {
         entry,
         pipeline: stages,
         namedMeasures,
         followed: new Set(),
         fromStage,
         base:
            fromStage === 0
               ? entry
               : (stages[fromStage - 1]?.outputStruct ?? {}),
      };
      ctx.pipeline.forEach((stage, i) => {
         if (i >= fromStage) this.stageFilters(ctx, stage, i);
      });
   }

   /**
    * One stage's terms and its fields' filters: stage `i` of the query reads
    * the entry point (0) or the previous stage's output, as a nest in it
    * does in its own first stage. `prior`: the stages whose output this
    * stage reads, from the entry point. A nest's later stage (`nested`) is
    * read as unresolved unless it compares no string.
    */
   private stageFilters(
      ctx: FilterContext,
      stage: QueryStage,
      i: number,
      prior: readonly QueryStage[] = ctx.pipeline.slice(0, i),
      nested = false,
   ): void {
      const term = (raw: unknown) => {
         // A `having:` names the stage's own outputs: read each as what it is.
         // A later stage's input is an earlier stage's output, whose fields
         // carry no expression of their own: an aggregate output there is
         // read as what it reads, for `stageTerm` to follow back through
         // the earlier stages.
         const input = nested
            ? undefined
            : prior.length === 0
              ? ctx.entry
              : LATER_STAGE;
         const e = this.outputsResolved(raw, stage, input);
         if (nested) this.nestTerm(ctx, e, prior);
         else if (prior.length === 0) this.entryTerm(ctx, e, []);
         else this.stageTerm(ctx, e, i);
      };
      for (const f of asArray(stage.filterList)) term((f as { e?: unknown }).e);
      for (const qf of asArray(stage.queryFields) as (FieldLike & {
         pipeline?: unknown;
      })[]) {
         if (qf.type === "turtle" && Array.isArray(qf.pipeline)) {
            const nest = qf.pipeline as QueryStage[];
            nest.forEach((s, j) =>
               j === 0
                  ? this.stageFilters(ctx, s, i, prior, nested)
                  : this.stageFilters(
                       ctx,
                       s,
                       j,
                       [...prior, ...nest.slice(0, j)],
                       true,
                    ),
            );
         } else if (qf.type === "fieldref" && Array.isArray(qf.path)) {
            if (prior.length === 0 && !nested) {
               this.namedMeasure(ctx, qf.path as string[]);
            }
         } else if (qf.e !== undefined) {
            for (const t of filteredTerms(qf.e)) term(t);
            if (prior.length === 0 && !nested) {
               for (const p of fieldPathsIn(qf.e)) this.namedMeasure(ctx, p);
            }
         }
      }
   }

   /**
    * `e` with each of `stage`'s outputs it names (`node: "outputField"`)
    * replaced by what that output is: the field it references, or its own
    * expression. One that is neither is left as it is, and read unresolved.
    * `input`: what the stage reads, on which an aggregate output is judged.
    */
   private outputsResolved(
      e: unknown,
      stage: QueryStage,
      input: unknown,
      depth = 0,
   ): unknown {
      if (depth > 64 || e === null || typeof e !== "object") return e;
      if (Array.isArray(e)) {
         return e.map((x) => this.outputsResolved(x, stage, input, depth + 1));
      }
      const n = e as { node?: unknown; name?: unknown };
      if (n.node === "outputField") {
         const qf = this.stageFields(stage).get(String(n.name));
         if (qf?.type === "fieldref" && Array.isArray(qf.path)) {
            return { node: "field", path: qf.path };
         }
         // An aggregate that is not a string, and whose value compares none
         // on the stage's input (a sum of a `pick` over a string ordering
         // compares one), compares as a number does; its own filters are
         // read from the stage's fields. Any other is read as what it is.
         const aggregate =
            qf?.e !== undefined &&
            (qf as { expressionType?: unknown }).expressionType ===
               "aggregate" &&
            nonStringType(qf.type);
         if (aggregate && input === LATER_STAGE) {
            const reads = this.laterAggregateReads(qf!.e, stage);
            if (reads !== undefined) return reads;
         } else if (
            aggregate &&
            input !== undefined &&
            this.valueComparesNoString(qf!.e, input)
         ) {
            return { node: "numberLiteral", literal: "0" };
         }
         if (qf?.e !== undefined) {
            return this.outputsResolved(qf.e, stage, input, depth + 1);
         }
         this.unresolved = true;
         return e;
      }
      return Object.fromEntries(
         Object.entries(e).map(([k, v]) => [
            k,
            this.outputsResolved(v, stage, input, depth + 1),
         ]),
      );
   }

   /**
    * What an aggregate output of a later stage reads, as one term the
    * earlier stages can be followed back through: each field it aggregates,
    * each of its filters' terms, and the stage's grouping keys (a count
    * grouped by a string counts what that string's comparison groups), as
    * `stageOutputsCompareNoString` reads a stage's aggregate. Undefined when
    * a key is not a plain expression (a window function, a nest).
    */
   private laterAggregateReads(
      e: unknown,
      stage: QueryStage,
   ): unknown | undefined {
      const parts: unknown[] = [];
      if (!this.aggregateReads(e, undefined, [], parts, 0)) return undefined;
      for (const qf of asArray(stage.queryFields) as (FieldLike & {
         expressionType?: unknown;
      })[]) {
         if (qf.expressionType === "aggregate") continue;
         if (qf.type === "fieldref" && Array.isArray(qf.path)) {
            parts.push({ node: "field", path: qf.path });
         } else if (
            qf.e !== undefined &&
            (qf.expressionType === undefined || qf.expressionType === "scalar")
         ) {
            parts.push(qf.e);
         } else {
            return undefined;
         }
      }
      return { node: "()", parts };
   }

   /** A term on the entry point's fields, read from the join at `prefix`. */
   private entryTerm(ctx: FilterContext, e: unknown, prefix: string[]): void {
      const owner =
         prefix.length === 0 ? ctx.entry : fieldAt(ctx.entry, prefix);
      const paths = fieldPathsIn(e).map((p) => [...prefix, ...p]);
      // A measure's value compares as a number does; its own filters are
      // followed as terms of their own.
      const measure = (p: string[]) => {
         const f = fieldAt(ctx.entry, p);
         return f?.expressionType === "aggregate" && nonStringType(f.type);
      };
      for (const p of paths) if (measure(p)) this.namedMeasure(ctx, p);
      if (this.comparesNoStringThroughScalars(e, owner)) return;
      this.scoped = true;
      this.comparesNoString = false;
      if (!this.onlyColumnComparisons(e, owner))
         this.comparesOnlyColumns = false;
      if (!this.equalityOrNotString(e, owner)) this.equalityOnly = false;
      for (const p of paths) {
         // A measure whose value compares no string has its filters followed
         // as terms of their own; any other is read as a field is.
         if (
            measure(p) &&
            this.valueComparesNoString(
               fieldAt(ctx.entry, p)?.e,
               p.length === 1 ? ctx.entry : fieldAt(ctx.entry, p.slice(0, -1)),
            )
         ) {
            continue;
         }
         this.entryPaths.push(p);
         this.resolve(ctx.entry, p, [], [], true);
      }
   }

   /**
    * Whether a measure's value compares no string: each filter it applies
    * and what it aggregates and combines ({@link valueComparesNoString}).
    */
   private measureComparesNoString(
      e: unknown,
      source: unknown,
      depth = 0,
   ): boolean {
      if (depth > 32) return false;
      return (
         filteredTerms(e).every((t) =>
            this.comparesNoStringThroughScalars(t, source, depth + 1),
         ) && this.valueComparesNoString(e, source, depth + 1)
      );
   }

   /**
    * Whether a measure's expression, its filters aside, compares no string:
    * an aggregate of a field read as it is, or of an expression comparing no
    * string (`sum(pick 1 when category > 'M' else 0)` compares one), and
    * arithmetic over those and over other measures, each read through its
    * own definition, filters included. Anything else reads false.
    */
   private valueComparesNoString(
      e: unknown,
      source: unknown,
      depth = 0,
   ): boolean {
      if (depth > 32 || e === null || typeof e !== "object") return false;
      const n = e as {
         node?: unknown;
         path?: unknown;
         e?: unknown;
         kids?: Record<string, unknown>;
      };
      switch (n.node) {
         case "numberLiteral":
         case "null":
            return true;
         case "aggregate": {
            const arg = n.e as { node?: unknown } | undefined;
            // `count()` aggregates no field.
            if (arg === undefined || arg.node === "") return true;
            // A sum or an average adds what it aggregates; any other
            // aggregate compares it (`count(category)` counts the distinct
            // values a collation tells apart, `max` orders them), so that
            // must be no string at all.
            const fn = (n as { function?: unknown }).function;
            return fn === "sum" || fn === "avg"
               ? this.valueComparesNoString(arg, source, depth + 1)
               : this.comparesNoStringThroughScalars(arg, source, depth + 1);
         }
         case "filteredExpr":
            return this.valueComparesNoString(n.kids?.e, source, depth + 1);
         case "field": {
            const path = n.path as string[];
            const f = Array.isArray(path) ? fieldAt(source, path) : undefined;
            if (f === undefined) return false;
            if (f.expressionType === "aggregate" && f.e !== undefined) {
               const owner =
                  path.length === 1
                     ? source
                     : fieldAt(source, path.slice(0, -1));
               return this.measureComparesNoString(f.e, owner, depth + 1);
            }
            // A value read as it is: its nulls and its value, as a null test
            // of it reads them (a computed one through what computes it).
            return this.comparesNoStringThroughScalars(
               { node: "is-null", e },
               source,
               depth + 1,
            );
         }
         case "+":
         case "-":
         case "*":
         case "/":
         case "%":
            return (
               this.valueComparesNoString(n.kids?.left, source, depth + 1) &&
               this.valueComparesNoString(n.kids?.right, source, depth + 1)
            );
         case "unary-":
         case "()":
            return this.valueComparesNoString(n.e, source, depth + 1);
         default:
            return this.comparesNoStringThroughScalars(e, source, depth + 1);
      }
   }

   /**
    * Whether `e` compares no string, reading each computed field it names
    * through what computes it: a dimension through its own expression, and
    * an output of a query-shaped source whose table the answer does not read
    * through the stage that computes it. A field's type says what it yields,
    * not what it compares (`is_books is category = 'books'` and `select: sb
    * is category = 'books'` are booleans), and `leavesNotString` reads only
    * the type. A measure is read through its definition
    * ({@link measureComparesNoString}).
    */
   private comparesNoStringThroughScalars(
      e: unknown,
      source: unknown,
      depth = 0,
   ): boolean {
      if (depth > 32 || !this.leavesNotString(e, source)) return false;
      return fieldPathsIn(e).every((p) => {
         const f = fieldAt(source, p);
         if (f === undefined) return false;
         const owner =
            p.length === 1 ? source : fieldAt(source, p.slice(0, -1));
         if (f.e !== undefined) {
            // A measure is judged at its definition, never taken on trust.
            return f.expressionType === "aggregate"
               ? this.measureComparesNoString(f.e, owner, depth + 1)
               : this.comparesNoStringThroughScalars(f.e, owner, depth + 1);
         }
         const query = (owner as { query?: QueryLike } | undefined)?.query;
         if (query === undefined || this.isStored(owner)) return true;
         const base = this.baseOf(query);
         const stages = asArray(query.pipeline) as QueryStage[];
         return (
            base !== undefined &&
            stages.length > 0 &&
            this.stageOutputsCompareNoString(
               { node: "field", path: [p[p.length - 1]] },
               stages,
               base,
               depth + 1,
            )
         );
      });
   }

   /**
    * Whether `e`, which names the outputs of the last of `stages` (a query
    * on `base`), compares no string: each output is replaced by what
    * computes it, back to `base`'s own fields. An aggregate reads what it
    * aggregates, its filters, and the stage's grouping keys (`n is count()`
    * grouped by a string counts what that string's comparison groups). An
    * output that cannot be followed (a nest, an analytic) compares a string.
    */
   private stageOutputsCompareNoString(
      e: unknown,
      stages: readonly QueryStage[],
      base: unknown,
      depth = 0,
   ): boolean {
      if (depth > 32) return false;
      if (stages.length === 0) {
         return this.comparesNoStringThroughScalars(e, base, depth);
      }
      const last = stages.length - 1;
      const input = last === 0 ? base : undefined;
      const outputs = asArray(stages[last].queryFields) as (FieldLike & {
         expressionType?: unknown;
      })[];
      const aggregate = (qf: (typeof outputs)[number]) =>
         qf.expressionType === "aggregate" ||
         (qf.type === "fieldref" &&
            Array.isArray(qf.path) &&
            fieldAt(input, qf.path as string[])?.expressionType ===
               "aggregate");
      const scalar = (qf: (typeof outputs)[number]): unknown => {
         if (qf.type === "fieldref" && Array.isArray(qf.path)) {
            return { node: "field", path: qf.path };
         }
         const kind = qf.expressionType;
         return qf.e !== undefined && (kind === undefined || kind === "scalar")
            ? qf.e
            : undefined;
      };
      const keys = outputs.filter((qf) => !aggregate(qf)).map(scalar);
      const byName = this.stageFields(stages[last]);
      const sub = withFields(e, (path) => {
         const qf = path.length === 1 ? byName.get(path[0]) : undefined;
         if (qf === undefined) return undefined;
         if (!aggregate(qf)) return scalar(qf);
         const parts: unknown[] = [];
         const measure =
            qf.type === "fieldref" ? { node: "field", path: qf.path } : qf.e;
         if (!this.aggregateReads(measure, input, [], parts, depth)) {
            return undefined;
         }
         if (keys.some((k) => k === undefined)) return undefined;
         return { node: "()", parts: [...parts, ...keys] };
      });
      return (
         sub !== undefined &&
         this.stageOutputsCompareNoString(
            sub,
            stages.slice(0, last),
            base,
            depth + 1,
         )
      );
   }

   /**
    * What an aggregate expression reads, into `out`: each field it
    * aggregates, a named measure through its own expression, and each
    * filter's terms; false when it is too deep to read.
    */
   private aggregateReads(
      e: unknown,
      source: unknown,
      prefix: readonly string[],
      out: unknown[],
      depth: number,
   ): boolean {
      if (depth > 32) return false;
      const under = (t: unknown) =>
         withFields(t, (p) => ({ node: "field", path: [...prefix, ...p] }));
      for (const t of filteredTerms(e)) out.push(under(t));
      for (const p of fieldPathsIn(e)) {
         const path = [...prefix, ...p];
         const f = fieldAt(source, path);
         if (f?.expressionType === "aggregate" && f.e !== undefined) {
            if (
               !this.aggregateReads(
                  f.e,
                  source,
                  path.slice(0, -1),
                  out,
                  depth + 1,
               )
            ) {
               return false;
            }
         } else {
            out.push({ node: "field", path });
         }
      }
      return true;
   }

   /** A term of a later stage, on the previous stage's output. */
   private stageTerm(ctx: FilterContext, e: unknown, i: number): void {
      const input = ctx.pipeline[i - 1]?.outputStruct;
      const prior = ctx.pipeline.slice(ctx.fromStage, i);
      if (this.stageOutputsCompareNoString(e, prior, ctx.base)) return;
      this.scoped = true;
      this.noColumnsOnly();
      if (!this.equalityOrNotString(e, input)) this.equalityOnly = false;
      const query = { query: { structRef: ctx.entry, pipeline: ctx.pipeline } };
      for (const p of fieldPathsIn(e)) {
         if (p.length !== 1) this.unresolved = true;
         else this.throughStages(query, p[0], [], [], i - 1, true);
      }
   }

   /**
    * A term of a nest's own later stage, on the stages before it (`prior`,
    * from the query's first): unresolved unless it compares no string.
    */
   private nestTerm(
      ctx: FilterContext,
      e: unknown,
      prior: readonly QueryStage[],
   ): void {
      const followed = prior.slice(ctx.fromStage);
      if (this.stageOutputsCompareNoString(e, followed, ctx.base)) return;
      this.scoped = true;
      this.noColumnsOnly();
      this.unresolved = true;
   }

   /** A measure the query reads by name: its filters, as the entry point's terms. */
   private namedMeasure(ctx: FilterContext, path: readonly string[]): void {
      if (!ctx.namedMeasures) return;
      const key = path.join(".");
      if (ctx.followed.has(key) || this.tooDeep()) return;
      ctx.followed.add(key);
      const f = fieldAt(ctx.entry, path);
      if (f?.expressionType !== "aggregate" || f.e === undefined) return;
      const prefix = path.slice(0, -1);
      for (const t of filteredTerms(f.e)) this.entryTerm(ctx, t, prefix);
      // A measure built from another reads that one's filters too.
      for (const p of fieldPathsIn(f.e))
         this.namedMeasure(ctx, [...prefix, ...p]);
   }
}

/**
 * {@link GateReads} for the gates (`exprs`) on `struct`, and every `where:`
 * that scopes the rows it reads. A name is followed through a rename to the
 * field it renames, through a computed field to every field its expression
 * reads, through a join to the joined source's field, and, on a query-shaped
 * source, back through each stage of its query to the fields of the source
 * the query runs on, until it reaches a column. `modelDef` resolves a
 * stage's source when it is named rather than inline. `stored` names (by
 * `sourceID`) the sources whose tables the answer reads: the query one runs
 * was applied by its build and is not a scope of the read; what is applied
 * when its table is read still is.
 *
 * The scope is every term that scopes the rows, not only one that reads a
 * given: a scope written as a literal (`acct = 9007199254740993`) merges
 * neighbours just the same.
 */
export function gateReads(
   struct: unknown,
   exprs: readonly string[],
   modelDef?: unknown,
   stored: ReadonlySet<string> = new Set(),
   /**
    * The gate texts the warehouse compares (the row filters): a lock is
    * decided in Publisher. Absent, every text is taken as one.
    */
   rowFilterExprs: readonly string[] = exprs,
   /** The sources the answer's compile re-rooted on their tables, read or not. */
   rerooted: ReadonlySet<string> = new Set(),
): GateReads {
   const walk = new GateReadWalk(modelDef, stored, rerooted);
   const read = gatePaths(exprs);
   if (read.unresolved) walk.unresolved = true;
   // A row filter is `field = $GIVEN` or `field in $GIVEN`: its field and
   // its given are the comparison's leaves.
   const compared = gatePaths(rowFilterExprs);
   if (
      compared.unresolved ||
      compared.paths.some(
         (p) =>
            !nonStringType(typeAt(struct, p)) ||
            !walk.joinsCompareNoString(struct, p),
      ) ||
      rowFilterExprs.some((expr) =>
         [...expr.matchAll(/\$([A-Za-z_][A-Za-z_0-9]*)/g)].some((m) => {
            const types = walk.givenTypesNamed(m[1]);
            return types.length === 0 || !types.every(nonStringType);
         }),
      )
   ) {
      walk.comparesNoString = false;
   }
   for (const p of read.paths) {
      walk.entryPaths.push(p);
      walk.resolve(struct, p, [], [], true);
   }
   walk.scope(struct, 0, (p) => walk.resolve(struct, p, [], [], true), true);
   return {
      names: walk.names,
      columns: walk.columns,
      columnOrigins: walk.columnOrigins,
      unresolved: walk.unresolved,
      scoped: walk.scoped,
      gated: exprs.length > 0,
      equalityOnly: walk.equalityOnly,
      entryPaths: walk.entryPaths,
      entryPathsComplete: walk.entryPathsComplete,
      comparesNoString: walk.comparesNoString && !walk.unresolved,
   };
}

/**
 * Every name the gates (`exprs`) and every `where:` term of `struct` read,
 * and every field each of those is computed from ({@link gateReads}), for
 * the readers that decline on a match.
 */
export function gateReadNames(
   struct: unknown,
   exprs: readonly string[],
   modelDef?: unknown,
): Set<string> {
   const names = new Set(gateReads(struct, exprs, modelDef).names);
   // Every identifier the text names too, fields or not: an extra name can
   // only send an answer live.
   for (const id of gateIdentifiers(exprs)) names.add(id);
   return names;
}

/**
 * The gates that apply to `struct` as an entry point, both routes, as the
 * governor walks them (own, inherited, a derivation's base), or `unreadable`
 * when the walk cannot read them: no struct or model to walk, a walk that
 * throws, or the walk's own fail-closed deny (an entry with no entry point to
 * graft on). Serve's one walk: every caller maps `unreadable` to a deny.
 */
export function entryGates(
   struct: unknown,
   modelDef: unknown,
): GateEntry[] | "unreadable" {
   if (struct === undefined || modelDef === undefined) return "unreadable";
   try {
      const entries = collectEntryPointGates(
         struct as SourceDef,
         modelDef as ModelDef,
         new Set(),
         true,
      );
      return entries.some((e) => e.struct === undefined)
         ? "unreadable"
         : entries;
   } catch {
      return "unreadable";
   }
}

/**
 * The gate texts that apply to `struct` as an entry point ({@link
 * entryGates}), both routes (a lock reads only givens and literals, so its
 * names never match a column), or undefined when the walk cannot read them:
 * a reader fails closed on that.
 */
export function entryPointGateExprs(
   struct: unknown,
   modelDef: unknown,
   /** Only the gates on this route; absent, both. */
   route?: string,
): string[] | undefined {
   const entries = entryGates(struct, modelDef);
   if (entries === "unreadable") return undefined;
   return entries
      .filter(
         (e) =>
            route === undefined || canonicalAuthorizeRoute(e.route) === route,
      )
      .flatMap((e) => [...e.exprs]);
}

type Schema = readonly { name: string; type: string }[];

/** The stored types of `schema`'s columns that hold one of the gates' columns. */
function typesIn(reads: GateReads, schema: Schema): string[] {
   const held = new Set(reads.columns.flat());
   return schema.filter((c) => held.has(c.name)).map((c) => c.type);
}

/**
 * The stored types of each table's gate columns, for the gate policy's
 * exactness rule (`gateColumnTypes` in `domain/gate_policy.ts`), by the
 * tables' order. Fail closed: when the gates cannot be read, or a column they
 * read is held by none of the tables (under any name it goes by), every
 * entry is undefined, which the policy reads as not exact on a `storage=`
 * table. A gate that reads no column lists none.
 */
export function gateColumnTypesOf(
   reads: GateReads | undefined,
   schemas: readonly (Schema | undefined)[],
): (string[] | undefined)[] {
   const none = schemas.map(() => undefined);
   if (reads === undefined || reads.unresolved) return none;
   if (schemas.some((s) => s === undefined)) return none;
   const held = new Set(schemas.flatMap((s) => (s ?? []).map((c) => c.name)));
   if (!reads.columns.every((alts) => alts.some((n) => held.has(n)))) {
      return none;
   }
   return schemas.map((s) => typesIn(reads, s ?? []));
}

/** A stored gate column's type in the warehouse table it copies. */
interface UpstreamType {
   dialect: string;
   nativeType: string;
}

/**
 * For each table, by the tables' order, and each of its gate columns in
 * {@link gateColumnTypesOf}'s order: the column's type in the warehouse
 * table it is a plain copy of (its `columnOrigins` entry, looked up in the
 * table's `upstreams`), or undefined when the build computed it, its origin
 * is not one column, or its table was not read. Undefined for a table
 * {@link gateColumnTypesOf} lists nothing for.
 */
export function gateColumnUpstreamTypesOf(
   reads: GateReads | undefined,
   schemas: readonly (Schema | undefined)[],
   upstreams: readonly (readonly UpstreamTableSchema[] | undefined)[],
): ((UpstreamType | undefined)[] | undefined)[] {
   const types = gateColumnTypesOf(reads, schemas);
   return schemas.map((schema, i) => {
      if (types[i] === undefined || reads === undefined) return undefined;
      const held = new Set(reads.columns.flat());
      return (schema ?? [])
         .filter((c) => held.has(c.name))
         .map((c) => upstreamTypeOf(reads, c.name, upstreams[i]));
   });
}

/** The upstream type of the stored column `name` (see gateColumnUpstreamTypesOf). */
function upstreamTypeOf(
   reads: GateReads,
   name: string,
   upstreams: readonly UpstreamTableSchema[] | undefined,
): UpstreamType | undefined {
   const origins = reads.columns.flatMap((names, j) =>
      names.includes(name) ? [reads.columnOrigins?.[j]] : [],
   );
   const origin = origins[0];
   if (
      origin === undefined ||
      origins.some(
         (o) =>
            o === undefined ||
            o.connectionName !== origin.connectionName ||
            o.table !== origin.table ||
            o.column !== origin.column,
      )
   ) {
      return undefined;
   }
   const table = upstreams?.find(
      (u) =>
         u.connectionName === origin.connectionName && u.table === origin.table,
   );
   if (!table?.columns || !table.dialect) return undefined;
   const exact = table.columns.filter((c) => c.name === origin.column);
   const folded = table.columns.filter(
      (c) => c.name.toLowerCase() === origin.column.toLowerCase(),
   );
   const column =
      exact.length === 1
         ? exact[0]
         : folded.length === 1
           ? folded[0]
           : undefined;
   return column
      ? { dialect: table.dialect, nativeType: column.nativeType }
      : undefined;
}

/**
 * {@link gateReads} for the source a compiled query runs on, read from the
 * author's compile (the live one), or undefined when it cannot be read.
 */
export async function gateReadsOfQuery(
   runnable: QueryMaterializer,
   /** The sources whose tables the answer reads, by `sourceID`. */
   stored: ReadonlySet<string> = new Set(),
   /**
    * The sources the answer's compile re-rooted on their tables, read or
    * not, by `sourceID`: a join to one never re-applies its own query.
    */
   rerooted: ReadonlySet<string> = new Set(),
): Promise<GateReads | undefined> {
   try {
      const prepared = await readPreparedQuery(runnable);
      if (prepared.modelDef === undefined) return undefined;
      const struct = resolveStructRefSource(
         prepared.structRef as never,
         prepared.modelDef as never,
      );
      const exprs = entryPointGateExprs(struct, prepared.modelDef);
      const rowFilters = entryPointGateExprs(
         struct,
         prepared.modelDef,
         ACCESS_FILTER_ROUTE,
      );
      return exprs === undefined || rowFilters === undefined
         ? undefined
         : gateReads(
              struct,
              exprs,
              prepared.modelDef,
              stored,
              rowFilters,
              rerooted,
           );
   } catch {
      return undefined;
   }
}

/**
 * What the caller's own query compares on the rows it reads, as
 * {@link GateReads} (never gated), from the author's compile, or undefined
 * when it cannot be read: the string comparisons its `where:`, `having:`,
 * filtered measures, views' and nests' filters make from stage `fromStage`
 * on, which an answer re-applies to a stored table. Judged by the same
 * collation rules as the entry point's scope (`gate_collation.ts`).
 */
export async function queryFilterReadsOfQuery(
   runnable: QueryMaterializer,
   /** The sources whose tables the answer reads, by `sourceID`. */
   stored: ReadonlySet<string>,
   options: {
      /** The first stage the answer applies to a stored table. */
      fromStage: number;
      /** Follow the filters of the measures the query names (not on a rollup). */
      namedMeasures: boolean;
      /**
       * The stages to read in place of the query's own, on its entry point:
       * a chained build's downstream query (`chainBuildCollationRefusal`).
       */
      pipeline?: readonly unknown[];
   },
): Promise<GateReads | undefined> {
   try {
      const prepared = await readPreparedQuery(runnable);
      const pipeline = options.pipeline ?? prepared.pipeline;
      if (!Array.isArray(pipeline)) return undefined;
      // A query with no filter needs no source to judge it by; one whose
      // source is unread reads each field it compares as unresolved.
      const struct =
         prepared.modelDef === undefined
            ? undefined
            : resolveStructRefSource(
                 prepared.structRef as never,
                 prepared.modelDef as never,
              );
      const walk = new GateReadWalk(prepared.modelDef, stored);
      walk.queryFilters(
         struct,
         pipeline,
         Math.max(0, options.fromStage),
         options.namedMeasures,
      );
      return {
         names: walk.names,
         columns: walk.columns,
         columnOrigins: walk.columnOrigins,
         unresolved: walk.unresolved,
         scoped: walk.scoped,
         gated: false,
         equalityOnly: walk.equalityOnly,
         entryPaths: walk.entryPaths,
         entryPathsComplete: walk.entryPathsComplete,
         comparesNoString: walk.comparesNoString && !walk.unresolved,
         comparesOnlyColumns: walk.comparesOnlyColumns && !walk.unresolved,
      };
   } catch {
      return undefined;
   }
}
