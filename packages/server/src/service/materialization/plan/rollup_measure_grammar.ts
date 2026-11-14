// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The measure grammar for `#(preaggregate)` (materialization v2, phase 4, A2):
 * whether a measure's partials can be stored per grain group and merged, read
 * from the measure's own text rather than from the compiled IR.
 *
 * A measure qualifies when it is built from mergeable aggregates and scalar
 * arithmetic over them:
 *
 *  - an aggregate leaf: `count()`, `x.sum()` or `sum(x)`, `x.min()`/`min(x)`,
 *    `x.max()`/`max(x)`, `j.count()` through a join, each optionally filtered
 *    (`amount.sum() { where: paid, region = 'x' }`);
 *  - a reference to another measure of the source, read through its own text
 *    (a filter on a reference applies to every aggregate inside it, as Malloy
 *    applies it);
 *  - numbers, strings, `true`/`false`/`null`, `+ - * / %`, comparisons, `and`,
 *    `or`, `not`, parentheses and scalar function calls over any of these
 *    (`coalesce(total, 0) * 2`, `revenue / order_count`).
 *
 * The rollup stores one partial per leaf (`count` merges with `sum`, never with
 * `count`), and the merged measure is the measure's own expression with each
 * leaf replaced by its partial's merge, so `revenue / order_count` stays a
 * ratio of totals and `coalesce(x.max(), 0)` applies after the merge, exactly
 * as live computes it.
 *
 * A measure that cannot be merged is refused (`not_mergeable`): `avg`, a
 * distinct count (`count(x)`), an ungrouped aggregate (`all`, `exclude`), or
 * any other aggregate of a row-level field. Text outside this grammar
 * (`pick`, a cast, a date literal, a `?` or `|` condition) is `unreadable`: it
 * may well be mergeable, but nothing here can prove it, so it is not stored.
 *
 * Every piece of generated text is re-emitted from the parse through
 * `malloy_text`: the author's expression is never spliced as a string.
 *
 * **Why text, and how it fails closed.** The build has to write each leaf
 * back as Malloy (`aggregate: total__partial is amount.sum() { where: … }`),
 * and Publisher writes Malloy only as text through `malloy_text`; the
 * compiled IR has no printer, and grafting IR would tie the build to
 * compiler internals. So the text is parsed, with the shared materialization
 * lexer (`malloy_lexer.ts`). The compiler stays the authority on what a
 * measure aggregates: the planner compares the leaves found here with the
 * compiled expression's aggregates (`governance/ir/measure_aggregates.ts`)
 * and stores nothing on any disagreement. Where the text is outside what is
 * read here, the measure is `unreadable` (served live), never refused at
 * publish; only a reading that is certain (`avg`, `count(x)`, `all`) is
 * `not_mergeable`.
 */

import { lexAll, type Token as LexToken } from "../malloy_lexer";
import {
   functionName,
   identifier,
   numberLiteral,
   stringLiteral,
} from "../../malloy_text";

/** The aggregates whose partials merge. */
type MergeableAggregate = "sum" | "count" | "min" | "max";

/** The aggregate applied to a stored partial to merge it. */
export type Reaggregate = "sum" | "min" | "max";

/** One node of a measure expression. */
export type MeasureExpr =
   | { kind: "number"; text: string }
   | { kind: "string"; value: string }
   | { kind: "literal"; value: "true" | "false" | "null" }
   | { kind: "path"; path: string[] }
   | { kind: "call"; name: string; args: MeasureExpr[] }
   | { kind: "method"; path: string[]; fn: string; args: MeasureExpr[] }
   | { kind: "filtered"; expr: MeasureExpr; where: MeasureExpr[] }
   | { kind: "unary"; op: "-" | "not"; expr: MeasureExpr }
   | { kind: "binary"; op: BinaryOp; left: MeasureExpr; right: MeasureExpr }
   | { kind: "paren"; expr: MeasureExpr }
   /** A stored partial, merged: only in a decomposed measure's `merged`. */
   | { kind: "partial"; column: string; reaggregate: Reaggregate };

type BinaryOp =
   | "+"
   | "-"
   | "*"
   | "/"
   | "%"
   | "="
   | "!="
   | "<"
   | "<="
   | ">"
   | ">="
   | "~"
   | "!~"
   | "and"
   | "or";

/** One aggregate leaf of a measure: what the build stores as one partial. */
export interface AggregateLeaf {
   aggregate: MergeableAggregate;
   /** The leaf as written, with every filter that applies to it. */
   expr: MeasureExpr;
   /**
    * The field paths the leaf reads: the method's path (`carriers.fleet` in
    * `carriers.fleet.sum()`), the argument's and the filters'. The planner
    * reads join locality from their heads.
    */
   reads: string[][];
   /** A join path the aggregate is taken at (`carriers` in `carriers.count()`). */
   joinPath: string[];
}

/** A measure, decomposed into partials and the merge that recombines them. */
export interface DecomposedMeasure {
   leaves: AggregateLeaf[];
   /** The measure with leaf `i` replaced by `{kind: "partial", column: columns[i]}`. */
   merged(columns: readonly string[]): MeasureExpr;
}

/** Why a measure cannot be pre-aggregated. */
export type MeasureRefusal =
   | { kind: "not_mergeable"; detail: string }
   | { kind: "unreadable"; detail: string };

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

type Token =
   | { t: "ident"; v: string }
   | { t: "number"; v: string }
   | { t: "string"; v: string }
   | { t: "op"; v: string };

class Unreadable extends Error {}

/** The operators the grammar reads; anything else is outside it. */
const OPERATORS = new Set([
   "!=",
   "<=",
   ">=",
   "!~",
   "=",
   "<",
   ">",
   "~",
   "+",
   "-",
   "*",
   "/",
   "%",
   "(",
   ")",
   "{",
   "}",
   ",",
   ".",
   ":",
]);

const NUMBER = /^[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?/;

/**
 * The grammar's tokens, read with the materialization lexer
 * (`malloy_lexer.ts`), the one lexer every Malloy-text reader here shares: it
 * splits words, brackets and quoted text, and skips comments; this joins its
 * one-character tokens back into numbers and two-character operators by
 * position, and decodes quoted text. Anything else is {@link Unreadable}.
 */
function tokenize(text: string): Token[] {
   const lexed = lexAll(text);
   if (!lexed) throw new Unreadable("an unbalanced bracket or quote");
   const out: Token[] = [];
   let k = 0;
   while (k < lexed.length) {
      const t = lexed[k] as LexToken;
      const c = t.text[0] as string;
      if (t.kind === "word") {
         out.push({ t: "ident", v: t.text });
         k++;
         continue;
      }
      if (c === "`" || c === "'" || c === '"') {
         if (t.text.startsWith('"""')) {
            throw new Unreadable("a triple-quoted string");
         }
         const value = unquote(t.text);
         out.push(
            c === "`" ? { t: "ident", v: value } : { t: "string", v: value },
         );
         k++;
         continue;
      }
      if (/[0-9]/.test(c)) {
         // `1.5e3` lexes as `1`, `.`, `5` and the word `e3`: take the
         // longest number at this offset and every token inside it.
         const m = NUMBER.exec(text.slice(t.start)) as RegExpExecArray;
         const end = t.start + m[0].length;
         while (k < lexed.length && (lexed[k] as LexToken).end <= end) k++;
         const next = lexed[k];
         if (next && next.start === end && next.kind === "word") {
            throw new Unreadable("a number");
         }
         out.push({ t: "number", v: m[0] });
         continue;
      }
      const next = lexed[k + 1];
      const pair =
         next && next.start === t.end ? `${t.text}${next.text}` : undefined;
      if (pair !== undefined && OPERATORS.has(pair)) {
         out.push({ t: "op", v: pair });
         k += 2;
         continue;
      }
      if (!OPERATORS.has(t.text)) throw new Unreadable(`\`${t.text}\``);
      out.push({ t: "op", v: t.text });
      k++;
   }
   return out;
}

/**
 * The value of a quoted token. Only the escapes whose meaning is plain; any
 * other is left to the compiler, not guessed here.
 */
function unquote(quoted: string): string {
   const q = quoted[0] as string;
   let value = "";
   for (let j = 1; j < quoted.length - 1; j++) {
      const d = quoted[j] as string;
      if (d !== "\\") {
         value += d;
         continue;
      }
      const e = quoted[j + 1];
      if (e === "\\" || e === q || e === "'" || e === '"') value += e;
      else if (e === "n") value += "\n";
      else if (e === "t") value += "\t";
      else if (e === "r") value += "\r";
      else throw new Unreadable("an escape");
      j++;
   }
   return value;
}

// ---------------------------------------------------------------------------
// Parser: or < and < not < comparison < additive < multiplicative < unary <
// postfix (`.f()`, `{ where: … }`) < primary
// ---------------------------------------------------------------------------

const KEYWORDS = new Set(["and", "or", "not", "true", "false", "null"]);

class Parser {
   private i = 0;
   constructor(private readonly tokens: Token[]) {}

   parseAll(): MeasureExpr {
      const e = this.or();
      if (this.i !== this.tokens.length) {
         throw new Unreadable(`\`${this.peek()?.v ?? ""}\``);
      }
      return e;
   }

   private peek(offset = 0): Token | undefined {
      return this.tokens[this.i + offset];
   }

   private isOp(v: string, offset = 0): boolean {
      const t = this.peek(offset);
      return t?.t === "op" && t.v === v;
   }

   private isWord(v: string): boolean {
      const t = this.peek();
      return t?.t === "ident" && t.v.toLowerCase() === v;
   }

   private expectOp(v: string): void {
      if (!this.isOp(v)) throw new Unreadable(`\`${this.peek()?.v ?? ""}\``);
      this.i++;
   }

   private or(): MeasureExpr {
      let left = this.and();
      while (this.isWord("or")) {
         this.i++;
         left = { kind: "binary", op: "or", left, right: this.and() };
      }
      return left;
   }

   private and(): MeasureExpr {
      let left = this.not();
      while (this.isWord("and")) {
         this.i++;
         left = { kind: "binary", op: "and", left, right: this.not() };
      }
      return left;
   }

   private not(): MeasureExpr {
      if (this.isWord("not")) {
         this.i++;
         return { kind: "unary", op: "not", expr: this.not() };
      }
      return this.comparison();
   }

   private comparison(): MeasureExpr {
      const left = this.additive();
      for (const op of ["=", "!=", "<=", ">=", "<", ">", "~", "!~"] as const) {
         if (this.isOp(op)) {
            this.i++;
            return { kind: "binary", op, left, right: this.additive() };
         }
      }
      return left;
   }

   private additive(): MeasureExpr {
      let left = this.multiplicative();
      for (;;) {
         const op = this.isOp("+") ? "+" : this.isOp("-") ? "-" : undefined;
         if (!op) return left;
         this.i++;
         left = { kind: "binary", op, left, right: this.multiplicative() };
      }
   }

   private multiplicative(): MeasureExpr {
      let left = this.unary();
      for (;;) {
         const op = this.isOp("*")
            ? "*"
            : this.isOp("/")
              ? "/"
              : this.isOp("%")
                ? "%"
                : undefined;
         if (!op) return left;
         this.i++;
         left = { kind: "binary", op, left, right: this.unary() };
      }
   }

   private unary(): MeasureExpr {
      if (this.isOp("-")) {
         this.i++;
         return { kind: "unary", op: "-", expr: this.unary() };
      }
      return this.postfix();
   }

   private postfix(): MeasureExpr {
      let e = this.primary();
      for (;;) {
         if (this.isOp("{")) {
            this.i++;
            const t = this.peek();
            if (!(t?.t === "ident" && t.v === "where") || !this.isOp(":", 1)) {
               throw new Unreadable("a refinement other than `where:`");
            }
            this.i += 2;
            const where = [this.or()];
            while (this.isOp(",")) {
               this.i++;
               where.push(this.or());
            }
            this.expectOp("}");
            e =
               e.kind === "filtered"
                  ? {
                       kind: "filtered",
                       expr: e.expr,
                       where: [...e.where, ...where],
                    }
                  : { kind: "filtered", expr: e, where };
            continue;
         }
         return e;
      }
   }

   private args(): MeasureExpr[] {
      this.expectOp("(");
      const args: MeasureExpr[] = [];
      if (!this.isOp(")")) {
         args.push(this.or());
         while (this.isOp(",")) {
            this.i++;
            args.push(this.or());
         }
      }
      this.expectOp(")");
      return args;
   }

   private primary(): MeasureExpr {
      const t = this.peek();
      if (!t) throw new Unreadable("the end of the expression");
      if (t.t === "number") {
         this.i++;
         return { kind: "number", text: t.v };
      }
      if (t.t === "string") {
         this.i++;
         return { kind: "string", value: t.v };
      }
      if (t.t === "op" && t.v === "(") {
         this.i++;
         const expr = this.or();
         this.expectOp(")");
         return { kind: "paren", expr };
      }
      if (t.t !== "ident") throw new Unreadable(`\`${t.v}\``);
      const word = t.v.toLowerCase();
      if (word === "true" || word === "false" || word === "null") {
         this.i++;
         return { kind: "literal", value: word };
      }
      if (KEYWORDS.has(word)) throw new Unreadable(`\`${t.v}\``);
      // A call: `f(…)`.
      if (this.isOp("(", 1)) {
         this.i++;
         return { kind: "call", name: t.v, args: this.args() };
      }
      // A path, possibly ending in a method call: `a.b`, `a.b.sum()`.
      const path = [t.v];
      this.i++;
      while (this.isOp(".")) {
         const next = this.peek(1);
         if (next?.t !== "ident") throw new Unreadable("a path");
         this.i += 2;
         if (this.isOp("(")) {
            return {
               kind: "method",
               path,
               fn: next.v,
               args: this.args(),
            };
         }
         path.push(next.v);
      }
      return { kind: "path", path };
   }
}

/** Parse a measure's text, or throw {@link Unreadable}. */
function parse(text: string): MeasureExpr {
   return new Parser(tokenize(text)).parseAll();
}

// ---------------------------------------------------------------------------
// Decomposition
// ---------------------------------------------------------------------------

/** Aggregate functions Malloy has that do not merge from partials. */
const NOT_MERGEABLE_AGGREGATES = new Set([
   "avg",
   "stddev",
   "variance",
   "string_agg",
   "string_agg_distinct",
   "count_distinct",
   "approx_count_distinct",
   "percentile_cont",
   "median",
]);

const REAGGREGATE: Record<MergeableAggregate, Reaggregate> = {
   sum: "sum",
   count: "sum",
   min: "min",
   max: "max",
};

/** What a measure's text resolves against: the source's own measures. */
interface MeasureScope {
   /** The source's measures, by name, with their text (the compiler's `code`). */
   measureCode(name: string): string | undefined;
   /** Whether a name is one of the source's joins. */
   isJoin?(name: string): boolean;
}

class NotMergeable extends Error {}

/** Every field path `e` reads (outside nested aggregates, which are refused). */
function pathsOf(e: MeasureExpr, out: string[][] = []): string[][] {
   switch (e.kind) {
      case "path":
         out.push(e.path);
         break;
      case "call":
         for (const a of e.args) pathsOf(a, out);
         break;
      case "method":
         out.push(e.path);
         for (const a of e.args) pathsOf(a, out);
         break;
      case "filtered":
         pathsOf(e.expr, out);
         for (const w of e.where) pathsOf(w, out);
         break;
      case "unary":
      case "paren":
         pathsOf(e.expr, out);
         break;
      case "binary":
         pathsOf(e.left, out);
         pathsOf(e.right, out);
         break;
      default:
         break;
   }
   return out;
}

/**
 * The aggregate a call or method names, when it is one Malloy computes from
 * rows: `mergeable` with its kind, `not_mergeable`, or undefined for a scalar
 * function.
 */
function aggregateOf(
   e: MeasureExpr,
   isJoin: (name: string) => boolean = () => false,
):
   | { mergeable: MergeableAggregate; joinPath: string[]; arg?: MeasureExpr }
   | { notMergeable: string }
   | undefined {
   if (e.kind === "method") {
      const fn = e.fn.toLowerCase();
      if (fn === "count") {
         // `j.count()` counts rows of join `j`. `x.count()` on a field is
         // not read here: the compiler takes it at `x`, which is neither the
         // source's rows nor a join, so it is left to serve live (never
         // refused, since nothing here can say it does not merge).
         if (e.args.length > 0) {
            throw new Unreadable(`\`${e.path.join(".")}.count(…)\``);
         }
         const head = e.path[0] as string;
         if (e.path.length === 1 && isJoin(head)) {
            return { mergeable: "count", joinPath: e.path };
         }
         throw new Unreadable(`\`${e.path.join(".")}.count()\``);
      }
      if (fn === "sum" || fn === "min" || fn === "max") {
         if (e.args.length > 0) throw new Unreadable(`\`.${e.fn}(…)\``);
         // `a.b.sum()` aggregates field `b` of join `a`.
         return { mergeable: fn, joinPath: e.path.slice(0, -1) };
      }
      if (NOT_MERGEABLE_AGGREGATES.has(fn)) {
         // malloy-text: a refusal message for a person, not Malloy
         return { notMergeable: `\`${e.fn}\`` };
      }
      throw new Unreadable(`\`.${e.fn}()\``);
   }
   if (e.kind === "call") {
      const fn = e.name.toLowerCase();
      if (fn === "count") {
         return e.args.length === 0
            ? { mergeable: "count", joinPath: [] }
            : { notMergeable: "a distinct count (`count(x)`)" };
      }
      if (fn === "sum" || fn === "min" || fn === "max") {
         if (e.args.length !== 1) throw new Unreadable(`\`${e.name}(…)\``);
         return { mergeable: fn, joinPath: [], arg: e.args[0] };
      }
      if (fn === "all" || fn === "exclude") {
         return { notMergeable: `an ungrouped aggregate (\`${e.name}\`)` };
      }
      if (NOT_MERGEABLE_AGGREGATES.has(fn)) {
         // malloy-text: a refusal message for a person, not Malloy
         return { notMergeable: `\`${e.name}\`` };
      }
   }
   return undefined;
}

/**
 * Decompose a measure's text into mergeable leaves, or say why it cannot be.
 * `name` is the measure's own name, for references and messages.
 */
export function decomposeMeasure(
   name: string,
   scope: MeasureScope,
): DecomposedMeasure | MeasureRefusal {
   const leaves: AggregateLeaf[] = [];
   const isJoin = (n: string) => scope.isJoin?.(n) ?? false;
   /** The leaf index each leaf node maps to, by identity in the rebuilt tree. */
   const leafSlots = new Map<MeasureExpr, number>();

   /** `e` rebuilt with leaves recorded; `filters` apply to every leaf below. */
   const walk = (
      e: MeasureExpr,
      filters: MeasureExpr[],
      expanding: ReadonlySet<string>,
   ): MeasureExpr => {
      switch (e.kind) {
         case "number":
         case "string":
         case "literal":
            return e;
         case "path": {
            const ref = e.path.length === 1 ? e.path[0] : undefined;
            const code = ref !== undefined ? scope.measureCode(ref) : undefined;
            if (ref === undefined || code === undefined) {
               // A joined source's measure, or a field read where the grammar
               // does not know what it is: not provably mergeable, and not
               // provably not, so the measure serves live (never refused).
               throw new Unreadable(
                  `\`${e.path.join(".")}\`, read outside sum, count, min or max`,
               );
            }
            if (expanding.has(ref)) {
               throw new Unreadable(
                  `a measure that refers to itself (\`${ref}\`)`,
               );
            }
            const inner = parse(code);
            return {
               kind: "paren",
               expr: walk(inner, filters, new Set([...expanding, ref])),
            };
         }
         case "filtered":
            return walk(e.expr, [...filters, ...e.where], expanding);
         case "call":
         case "method": {
            const agg = aggregateOf(e, isJoin);
            if (agg && "notMergeable" in agg) {
               throw new NotMergeable(agg.notMergeable);
            }
            if (agg) {
               const leafExpr: MeasureExpr =
                  filters.length > 0
                     ? { kind: "filtered", expr: e, where: filters }
                     : e;
               // A nested aggregate (`sum(x.sum())`) is not a row-level leaf.
               const inside = e.kind === "call" ? e.args : e.args;
               if (inside.some(containsAggregate)) {
                  throw new NotMergeable("an aggregate of an aggregate");
               }
               if (filters.some(containsAggregate)) {
                  throw new NotMergeable("a filter that reads an aggregate");
               }
               leaves.push({
                  aggregate: agg.mergeable,
                  expr: leafExpr,
                  reads: pathsOf(leafExpr),
                  joinPath: agg.joinPath,
               });
               const slot: MeasureExpr = { kind: "literal", value: "null" };
               leafSlots.set(slot, leaves.length - 1);
               return slot;
            }
            if (e.kind === "method") {
               throw new Unreadable(`\`.${e.fn}()\``);
            }
            // A scalar function over the measure's values.
            return {
               kind: "call",
               name: e.name,
               args: e.args.map((a) => walk(a, filters, expanding)),
            };
         }
         case "unary":
            return { ...e, expr: walk(e.expr, filters, expanding) };
         case "paren":
            return { ...e, expr: walk(e.expr, filters, expanding) };
         case "binary":
            return {
               ...e,
               left: walk(e.left, filters, expanding),
               right: walk(e.right, filters, expanding),
            };
         case "partial":
            throw new Unreadable("a partial");
      }
   };

   const code = scope.measureCode(name);
   if (code === undefined) {
      return { kind: "not_mergeable", detail: "it is not a measure" };
   }
   let skeleton: MeasureExpr;
   try {
      skeleton = walk(parse(code), [], new Set([name]));
   } catch (error) {
      if (error instanceof NotMergeable) {
         return { kind: "not_mergeable", detail: error.message };
      }
      if (error instanceof Unreadable) {
         return { kind: "unreadable", detail: error.message };
      }
      throw error;
   }
   if (leaves.length === 0) {
      return { kind: "not_mergeable", detail: "it aggregates nothing" };
   }
   return {
      leaves,
      merged(columns) {
         const fill = (e: MeasureExpr): MeasureExpr => {
            const slot = leafSlots.get(e);
            if (slot !== undefined) {
               const leaf = leaves[slot] as AggregateLeaf;
               return {
                  kind: "partial",
                  column: columns[slot] as string,
                  reaggregate: REAGGREGATE[leaf.aggregate],
               };
            }
            switch (e.kind) {
               case "call":
                  return { ...e, args: e.args.map(fill) };
               case "unary":
               case "paren":
                  return { ...e, expr: fill(e.expr) };
               case "binary":
                  return { ...e, left: fill(e.left), right: fill(e.right) };
               default:
                  return e;
            }
         };
         return fill(skeleton);
      },
   };
}

/** Whether `e` contains an aggregate call anywhere. */
function containsAggregate(e: MeasureExpr): boolean {
   switch (e.kind) {
      case "call":
         return aggregateOf(e) !== undefined || e.args.some(containsAggregate);
      case "method":
         return true;
      case "filtered":
         return containsAggregate(e.expr) || e.where.some(containsAggregate);
      case "unary":
      case "paren":
         return containsAggregate(e.expr);
      case "binary":
         return containsAggregate(e.left) || containsAggregate(e.right);
      default:
         return false;
   }
}

/** Whether `d` is a refusal rather than a decomposition. */
export function isMeasureRefusal(
   d: DecomposedMeasure | MeasureRefusal,
): d is MeasureRefusal {
   return "kind" in d;
}

// ---------------------------------------------------------------------------
// Emission, through malloy_text
// ---------------------------------------------------------------------------

function binaryOpText(op: BinaryOp): string {
   switch (op) {
      case "+":
         return "+";
      case "-":
         return "-";
      case "*":
         return "*";
      case "/":
         return "/";
      case "%":
         return "%";
      case "=":
         return "=";
      case "!=":
         return "!=";
      case "<":
         return "<";
      case "<=":
         return "<=";
      case ">":
         return ">";
      case ">=":
         return ">=";
      case "~":
         return "~";
      case "!~":
         return "!~";
      case "and":
         return "and";
      case "or":
         return "or";
   }
}

function literalText(value: "true" | "false" | "null"): string {
   switch (value) {
      case "true":
         return "true";
      case "false":
         return "false";
      case "null":
         return "null";
   }
}

function aggregateMethodText(fn: string): string {
   switch (fn.toLowerCase()) {
      case "sum":
         return "sum";
      case "count":
         return "count";
      case "min":
         return "min";
      case "max":
         return "max";
      default:
         throw new Error(`not an aggregate the grammar emits: ${fn}`);
   }
}

function reaggregateText(r: Reaggregate): string {
   switch (r) {
      case "sum":
         return "sum";
      case "min":
         return "min";
      case "max":
         return "max";
   }
}

/** A field path, each segment through `identifier`. */
function pathText(path: readonly string[]): string {
   return path.map((segment) => identifier(segment)).join(".");
}

/** Malloy text for `e`, every name and value through `malloy_text`. */
export function emitMeasureExpr(e: MeasureExpr): string {
   switch (e.kind) {
      case "number":
         return numberLiteral(e.text);
      case "string":
         return stringLiteral(e.value);
      case "literal":
         return literalText(e.value);
      case "path":
         return pathText(e.path);
      case "call":
         return `${functionName(e.name)}(${e.args.map((a) => emitMeasureExpr(a)).join(", ")})`;
      case "method":
         return `${pathText(e.path)}.${aggregateMethodText(e.fn)}(${e.args.map((a) => emitMeasureExpr(a)).join(", ")})`;
      case "filtered":
         return `${emitMeasureExpr(e.expr)} { where: ${e.where.map((w) => emitMeasureExpr(w)).join(", ")} }`;
      case "unary": {
         const operand = emitMeasureExpr(e.expr);
         if (e.op === "not") return `not ${operand}`;
         // `--` starts a Malloy comment: keep two minus signs apart.
         return operand.startsWith("-") ? `- ${operand}` : `-${operand}`;
      }
      case "binary":
         return `${emitMeasureExpr(e.left)} ${binaryOpText(e.op)} ${emitMeasureExpr(e.right)}`;
      case "paren":
         return `(${emitMeasureExpr(e.expr)})`;
      case "partial":
         return `${identifier(e.column)}.${reaggregateText(e.reaggregate)}()`;
   }
}
