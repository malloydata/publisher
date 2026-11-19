// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What an entry point's row-level gates read from a stored table, for the
 * checks that decide whether a routed answer filters the stored rows as live
 * filters the source's: a query's own time zone (a gate over a stored
 * truncation, `serve/served_copy_serve.ts`, `serve/rollup_variants.ts`) and a
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
import { collectEntryPointGates } from "../../gate_classification";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import { stripMalloyCommentsAndLiterals } from "../../malloy_lexer_core";
import { malloyIdentifiers } from "../../query_text";

import { fieldPathsIn } from "./gate_paths";
import type { GateReads } from "./gate_reads_types";

// The comparison rule beside the column rules, read by the router through
// this module.
export {
   storedTableCollationRefusal,
   storedTypeIsString,
   stringGateCollationRefusal,
} from "./gate_collation";

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
}

/** What the query-filter walk carries through one query. */
interface FilterContext {
   readonly entry: unknown;
   readonly pipeline: readonly QueryStage[];
   readonly namedMeasures: boolean;
   /** The named measures already followed, by path. */
   readonly followed: Set<string>;
}

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

/** A query-shaped source's query, as the walk reads it. */
interface QueryLike {
   structRef?: unknown;
   pipeline?: unknown;
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
   unresolved = false;
   scoped = false;
   equalityOnly = true;
   comparesNoString = true;
   readonly entryPaths: string[][] = [];
   entryPathsComplete = true;
   private steps = 0;

   constructor(
      private readonly modelDef: unknown,
      private readonly stored: ReadonlySet<string>,
   ) {}

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
   leavesNotString(e: unknown, source: unknown, depth = 0): boolean {
      if (depth > 64 || e === null || typeof e !== "object") return false;
      const n = e as { node?: unknown; path?: unknown; id?: unknown };
      switch (n.node) {
         case "field":
            return (
               Array.isArray(n.path) &&
               nonStringType(typeAt(source, n.path as string[]))
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
               if (!this.leavesNotString(o, source, depth + 1)) return false;
            }
         }
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
         if (!this.leavesNotString(f.e, source)) this.comparesNoString = false;
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

   /** One stage's step back: what each wanted output reads in its input. */
   private stepBack(
      byName: Map<string, FieldLike>,
      wanted: Wanted[],
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
         } else if (qf.e !== undefined) {
            if (!equalityOnlyExpr(qf.e)) this.equalityOnly = false;
            // A stage's expression reads the stage's input, not typed here,
            // and has no path from the entry point.
            if (!this.leavesNotString(qf.e, undefined)) {
               this.comparesNoString = false;
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
         wanted = this.stepBack(this.stageFields(stages[i]), wanted);
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
         if (!this.leavesNotString(e, source)) this.comparesNoString = false;
         for (const p of fieldPathsIn(e)) {
            if (depth === 0) this.entryPaths.push(p);
            else this.entryPathsComplete = false;
            at(p);
         }
      }
      this.scopeMembers(source, depth, at, own);
      this.scopeJoins(source, depth, at);
      if (s.query !== undefined && !this.isStored(source)) {
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

   /** Each joined source's own `where:`, which filters the rows the join reaches. */
   private scopeJoins(
      source: unknown,
      depth: number,
      at: (path: readonly string[]) => void,
   ): void {
      const fields = (source as { fields?: unknown }).fields;
      for (const f of (Array.isArray(fields) ? fields : []) as FieldLike[]) {
         if (f.join === undefined) continue;
         const name = activeName(f);
         this.scope(f, depth + 1, (p) => at([name, ...p]), false);
      }
   }

   /**
    * A query-shaped source's base's terms and each stage's own, a later
    * stage's read back through the earlier ones.
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
         const where = (stage as { filterList?: unknown }).filterList;
         for (const f of Array.isArray(where) ? where : []) {
            this.scoped = true;
            if (!equalityOnlyExpr((f as { e?: unknown }).e)) {
               this.equalityOnly = false;
            }
            this.entryPathsComplete = false;
            // A stage's term reads the stage's fields, not typed here.
            this.comparesNoString = false;
            for (const p of fieldPathsIn((f as { e?: unknown }).e)) {
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
      const ctx: FilterContext = {
         entry,
         pipeline: pipeline as QueryStage[],
         namedMeasures,
         followed: new Set(),
      };
      ctx.pipeline.forEach((stage, i) => {
         if (i >= fromStage) this.stageFilters(ctx, stage, i);
      });
   }

   /**
    * One stage's terms and its fields' filters: stage `i` of the query reads
    * the entry point (0) or the previous stage's output, as a nest in it
    * does in its own first stage. A nest's later stage (`nestInput`, its
    * input) is read as unresolved unless it compares no string.
    */
   private stageFilters(
      ctx: FilterContext,
      stage: QueryStage,
      i: number,
      nestInput?: unknown,
   ): void {
      const term = (raw: unknown) => {
         // A `having:` names the stage's own outputs: read each as what it is.
         const e = this.outputsResolved(raw, stage);
         if (nestInput !== undefined) this.nestTerm(e, nestInput);
         else if (i === 0) this.entryTerm(ctx, e, []);
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
                  ? this.stageFilters(ctx, s, i, nestInput)
                  : this.stageFilters(
                       ctx,
                       s,
                       j,
                       nest[j - 1].outputStruct ?? {},
                    ),
            );
         } else if (qf.type === "fieldref" && Array.isArray(qf.path)) {
            if (i === 0 && nestInput === undefined) {
               this.namedMeasure(ctx, qf.path as string[]);
            }
         } else if (qf.e !== undefined) {
            for (const t of filteredTerms(qf.e)) term(t);
            if (i === 0 && nestInput === undefined) {
               for (const p of fieldPathsIn(qf.e)) this.namedMeasure(ctx, p);
            }
         }
      }
   }

   /**
    * `e` with each of `stage`'s outputs it names (`node: "outputField"`)
    * replaced by what that output is: the field it references, or its own
    * expression. One that is neither is left as it is, and read unresolved.
    */
   private outputsResolved(e: unknown, stage: QueryStage, depth = 0): unknown {
      if (depth > 64 || e === null || typeof e !== "object") return e;
      if (Array.isArray(e)) {
         return e.map((x) => this.outputsResolved(x, stage, depth + 1));
      }
      const n = e as { node?: unknown; name?: unknown };
      if (n.node === "outputField") {
         const qf = this.stageFields(stage).get(String(n.name));
         if (qf?.type === "fieldref" && Array.isArray(qf.path)) {
            return { node: "field", path: qf.path };
         }
         // An aggregate that is not a string compares as a number does (its
         // own filters are read from the stage's fields).
         if (
            qf?.e !== undefined &&
            (qf as { expressionType?: unknown }).expressionType ===
               "aggregate" &&
            nonStringType(qf.type)
         ) {
            return { node: "numberLiteral", literal: "0" };
         }
         if (qf?.e !== undefined) {
            return this.outputsResolved(qf.e, stage, depth + 1);
         }
         this.unresolved = true;
         return e;
      }
      return Object.fromEntries(
         Object.entries(e).map(([k, v]) => [
            k,
            this.outputsResolved(v, stage, depth + 1),
         ]),
      );
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
      const computedScalar = paths.some((p) => {
         const f = fieldAt(ctx.entry, p);
         return f === undefined || (f.e !== undefined && !measure(p));
      });
      if (!computedScalar && this.leavesNotString(e, owner)) return;
      this.scoped = true;
      this.comparesNoString = false;
      if (!this.equalityOrNotString(e, owner)) this.equalityOnly = false;
      for (const p of paths) {
         if (measure(p)) continue;
         this.entryPaths.push(p);
         this.resolve(ctx.entry, p, [], [], true);
      }
   }

   /** A term of a later stage, on the previous stage's output. */
   private stageTerm(ctx: FilterContext, e: unknown, i: number): void {
      const input = ctx.pipeline[i - 1]?.outputStruct;
      if (this.leavesNotString(e, input)) return;
      this.scoped = true;
      this.comparesNoString = false;
      if (!this.equalityOrNotString(e, input)) this.equalityOnly = false;
      const query = { query: { structRef: ctx.entry, pipeline: ctx.pipeline } };
      for (const p of fieldPathsIn(e)) {
         if (p.length !== 1) this.unresolved = true;
         else this.throughStages(query, p[0], [], [], i - 1, true);
      }
   }

   /** A term of a nest's own later stage: unresolved unless it compares no string. */
   private nestTerm(e: unknown, input: unknown): void {
      if (this.leavesNotString(e, input)) return;
      this.scoped = true;
      this.comparesNoString = false;
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
): GateReads {
   const walk = new GateReadWalk(modelDef, stored);
   const read = gatePaths(exprs);
   if (read.unresolved) walk.unresolved = true;
   // A row filter is `field = $GIVEN` or `field in $GIVEN`: its field and
   // its given are the comparison's leaves.
   const compared = gatePaths(rowFilterExprs);
   if (
      compared.unresolved ||
      compared.paths.some((p) => !nonStringType(typeAt(struct, p))) ||
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
 * The gate texts that apply to `struct` as an entry point, both routes (a
 * lock reads only givens and literals, so its names never match a column),
 * or undefined when the walk cannot read them: a reader fails closed on that.
 */
export function entryPointGateExprs(
   struct: unknown,
   modelDef: unknown,
   /** Only the gates on this route; absent, both. */
   route?: string,
): string[] | undefined {
   if (struct === undefined || modelDef === undefined) return undefined;
   try {
      const entries = collectEntryPointGates(
         struct as SourceDef,
         modelDef as ModelDef,
         new Set(),
         true,
      );
      // The walk's fail-closed deny (no entry point to graft on).
      if (entries.some((e) => e.struct === undefined)) return undefined;
      return entries
         .filter(
            (e) =>
               route === undefined ||
               canonicalAuthorizeRoute(e.route) === route,
         )
         .flatMap((e) => [...e.exprs]);
   } catch {
      return undefined;
   }
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

/**
 * {@link gateReads} for the source a compiled query runs on, read from the
 * author's compile (the live one), or undefined when it cannot be read.
 */
export async function gateReadsOfQuery(
   runnable: QueryMaterializer,
   /** The sources whose tables the answer reads, by `sourceID`. */
   stored: ReadonlySet<string> = new Set(),
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
         : gateReads(struct, exprs, prepared.modelDef, stored, rowFilters);
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
   },
): Promise<GateReads | undefined> {
   try {
      const prepared = await readPreparedQuery(runnable);
      if (!Array.isArray(prepared.pipeline)) return undefined;
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
         prepared.pipeline,
         Math.max(0, options.fromStage),
         options.namedMeasures,
      );
      return {
         names: walk.names,
         columns: walk.columns,
         unresolved: walk.unresolved,
         scoped: walk.scoped,
         gated: false,
         equalityOnly: walk.equalityOnly,
         entryPaths: walk.entryPaths,
         entryPathsComplete: walk.entryPathsComplete,
         comparesNoString: walk.comparesNoString && !walk.unresolved,
      };
   } catch {
      return undefined;
   }
}
