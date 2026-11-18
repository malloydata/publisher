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
import { collectEntryPointGates } from "../../gate_classification";
import {
   readPreparedQuery,
   resolveStructRefSource,
} from "../../governance/ir/prepared_query";
import { stripMalloyCommentsAndLiterals } from "../../malloy_lexer_core";
import { malloyIdentifiers } from "../../query_text";

import type { GateReads } from "./gate_reads_types";

// The comparison rule beside the column rules, read by the router through
// this module.
export {
   storedTableCollationRefusal,
   storedTypeIsString,
   stringGateCollationRefusal,
} from "./gate_collation";

/** Every field path an expression reads (`node: "field"`), at any depth. */
export function fieldPathsIn(
   e: unknown,
   out: string[][] = [],
   depth = 0,
): string[][] {
   if (depth > 64 || e === null || typeof e !== "object") return out;
   if (Array.isArray(e)) {
      for (const x of e) fieldPathsIn(x, out, depth + 1);
      return out;
   }
   const n = e as { node?: unknown; path?: unknown };
   if (
      n.node === "field" &&
      Array.isArray(n.path) &&
      n.path.every((p) => typeof p === "string")
   ) {
      out.push(n.path as string[]);
   }
   for (const v of Object.values(e)) fieldPathsIn(v, out, depth + 1);
   return out;
}

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
   readonly entryPaths: string[][] = [];
   entryPathsComplete = true;
   private steps = 0;

   constructor(
      private readonly modelDef: unknown,
      private readonly stored: ReadonlySet<string>,
   ) {}

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

   /** Follow a field path of `source` to the columns it reads. */
   resolve(
      source: unknown,
      path: readonly string[],
      chain: readonly string[],
      joins: readonly string[] = [],
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
         );
         return;
      }
      this.field(source, f, chain, joins);
   }

   /** Follow one field: a rename, an expression, a stage output or a column. */
   private field(
      source: unknown,
      f: FieldLike,
      chain: readonly string[],
      joins: readonly string[],
   ): void {
      const name = activeName(f);
      const stored = typeof f.name === "string" ? f.name : name;
      this.names.add(name);
      this.names.add(stored);
      const along = [...chain, name, stored];
      if (f.e !== undefined) {
         if (!equalityOnlyExpr(f.e)) this.equalityOnly = false;
         for (const p of fieldPathsIn(f.e))
            this.resolve(source, p, along, joins);
         return;
      }
      if (f.join !== undefined) {
         // A gate comparing a join itself reads no column it can name.
         this.unresolved = true;
         return;
      }
      if ((source as { query?: unknown } | undefined)?.query !== undefined) {
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
      for (const w of wanted) this.resolve(base, w.path, w.chain, joins);
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
         if (!equalityOnlyExpr(e)) this.equalityOnly = false;
         for (const p of fieldPathsIn(e)) {
            if (depth === 0) this.entryPaths.push(p);
            else this.entryPathsComplete = false;
            at(p);
         }
      }
      this.scopeMembers(source, depth, at, own);
      this.scopeJoins(source, depth, at);
      const built =
         typeof s.sourceID === "string" && this.stored.has(s.sourceID);
      if (s.query !== undefined && !built) {
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
            for (const p of fieldPathsIn((f as { e?: unknown }).e)) {
               if (i === 0) atBase(p);
               else if (p.length === 1) {
                  this.throughStages(source, p[0], [], [], i - 1);
               } else this.unresolved = true;
            }
         }
      });
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
): GateReads {
   const walk = new GateReadWalk(modelDef, stored);
   const read = gatePaths(exprs);
   if (read.unresolved) walk.unresolved = true;
   for (const p of read.paths) {
      walk.entryPaths.push(p);
      walk.resolve(struct, p, []);
   }
   walk.scope(struct, 0, (p) => walk.resolve(struct, p, []), true);
   return {
      names: walk.names,
      columns: walk.columns,
      unresolved: walk.unresolved,
      scoped: walk.scoped,
      gated: exprs.length > 0,
      equalityOnly: walk.equalityOnly,
      entryPaths: walk.entryPaths,
      entryPathsComplete: walk.entryPathsComplete,
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
      return entries.flatMap((e) => [...e.exprs]);
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
      return exprs === undefined
         ? undefined
         : gateReads(struct, exprs, prepared.modelDef, stored);
   } catch {
      return undefined;
   }
}
