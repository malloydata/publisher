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

/** What an entry point's row-level gates read. */
export interface GateReads {
   /**
    * Every name the gates read, and every field those are computed from,
    * through renames, expressions, joins and query stages. Over-collected on
    * purpose, for the readers that decline on a match (the zone checks).
    */
   readonly names: ReadonlySet<string>;
   /**
    * Each stored column the gates read, as the names it goes by along the way
    * from the gate to the column (a rename's, a query stage's output, the
    * base column's), any of which a stored table may hold it under.
    */
   readonly columns: readonly (readonly string[])[];
   /**
    * A name the gates read that could not be followed to a column: a reader
    * that judges the columns must fail closed.
    */
   readonly unresolved: boolean;
   /**
    * The rows the query reads are scoped by a `where:` (the source's own, one
    * it extends, a composite member's, or a query-shaped source's base's or
    * stage's): its columns are read here as a gate's are.
    */
   readonly scoped: boolean;
   /** The entry point carries a gate: the gate policy judges these columns past it. */
   readonly gated: boolean;
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

/**
 * {@link GateReads} for the gates (`exprs`) on `struct`. A name is followed
 * through a rename to the field it renames, through a computed field to every
 * field its expression reads, through a join to the joined source's field,
 * and, on a query-shaped source, back through each stage of its query to the
 * fields of the source the query runs on, until it reaches a column.
 * `modelDef` resolves a stage's source when it is named rather than inline.
 */
export function gateReads(
   struct: unknown,
   exprs: readonly string[],
   modelDef?: unknown,
): GateReads {
   const names = new Set<string>();
   const columns: string[][] = [];
   let unresolved = false;
   let steps = 0;
   const tooDeep = () => ++steps > 4096;

   const resolve = (
      source: unknown,
      path: readonly string[],
      chain: readonly string[],
      joins: readonly string[] = [],
   ): void => {
      if (tooDeep() || path.length === 0) {
         unresolved = true;
         return;
      }
      const f = fieldsOf(source).get(path[0]);
      names.add(path[0]);
      if (f === undefined) {
         unresolved = true;
         return;
      }
      if (path.length > 1) {
         if (f.join === undefined) {
            unresolved = true;
            return;
         }
         // A stored table holds a joined source's fields under the join's
         // name: an inlined join's record, or flattened as `<join>__<field>`.
         resolve(f, path.slice(1), [...chain, path[0]], [...joins, path[0]]);
         return;
      }
      field(source, f, chain, joins);
   };

   const field = (
      source: unknown,
      f: FieldLike,
      chain: readonly string[],
      joins: readonly string[] = [],
   ): void => {
      const name = activeName(f);
      const stored = typeof f.name === "string" ? f.name : name;
      names.add(name);
      names.add(stored);
      const along = [...chain, name, stored];
      if (f.e !== undefined) {
         for (const p of fieldPathsIn(f.e)) resolve(source, p, along, joins);
         return;
      }
      if (f.join !== undefined) {
         // A gate comparing a join itself reads no column it can name.
         unresolved = true;
         return;
      }
      const query = (source as { query?: { pipeline?: unknown[] } } | undefined)
         ?.query;
      if (query !== undefined) {
         throughStages(source, stored, along, joins);
         return;
      }
      const flattened =
         joins.length === 0 ? [] : along.map((n) => [...joins, n].join("__"));
      columns.push([...new Set([...along, ...flattened])]);
   };

   /** Follow an output field of a query-shaped source back to its base. */
   const throughStages = (
      source: unknown,
      output: string,
      chain: readonly string[],
      joins: readonly string[],
      /** The stage whose output `output` names; the last one by default. */
      fromStage?: number,
   ): void => {
      const query = (
         source as {
            query?: {
               pipeline?: { queryFields?: unknown[] }[];
               structRef?: unknown;
            };
         }
      ).query!;
      const stages = Array.isArray(query.pipeline) ? query.pipeline : [];
      if (stages.length === 0) {
         unresolved = true;
         return;
      }
      let wanted: { path: string[]; chain: string[] }[] = [
         { path: [output], chain: [...chain] },
      ];
      for (let i = fromStage ?? stages.length - 1; i >= 0; i--) {
         const byName = new Map<string, FieldLike>();
         for (const qf of (stages[i].queryFields ?? []) as FieldLike[]) {
            if (qf.type === "fieldref" && Array.isArray(qf.path)) {
               const p = qf.path as string[];
               byName.set(p[p.length - 1], qf);
            } else if (qf.name !== undefined) {
               byName.set(activeName(qf), qf);
            }
         }
         const next: { path: string[]; chain: string[] }[] = [];
         for (const w of wanted) {
            if (w.path.length !== 1 || tooDeep()) {
               unresolved = true;
               continue;
            }
            const qf = byName.get(w.path[0]);
            names.add(w.path[0]);
            if (qf === undefined) {
               unresolved = true;
            } else if (qf.type === "fieldref" && Array.isArray(qf.path)) {
               const p = qf.path as string[];
               next.push({ path: p, chain: [...w.chain, w.path[0]] });
            } else if (qf.e !== undefined) {
               const read = fieldPathsIn(qf.e);
               if (read.length === 0) unresolved = true;
               for (const p of read) {
                  next.push({ path: p, chain: [...w.chain, w.path[0]] });
               }
            } else {
               unresolved = true;
            }
         }
         // An earlier stage's output is one name; only the first stage reads
         // the source, through a join path or a name.
         if (i > 0 && next.some((n) => n.path.length !== 1)) unresolved = true;
         wanted = next;
      }
      const ref = query.structRef;
      const base =
         typeof ref === "string"
            ? (modelDef as { contents?: Record<string, unknown> } | undefined)
                 ?.contents?.[ref]
            : ref;
      if (base === undefined || base === null) {
         unresolved = true;
         return;
      }
      for (const w of wanted) resolve(base, w.path, w.chain, joins);
   };

   const read = gatePaths(exprs);
   if (read.unresolved) unresolved = true;
   for (const p of read.paths) resolve(struct, p, []);
   // Every `where:` that scopes the rows the query reads: the source's own
   // (its `filterList` already holds the terms of each source it extends),
   // each member's of a composite, and, for a query-shaped source, its base's
   // and each stage's own, a later stage's read back through the earlier
   // ones. Every term, not only one that reads a given: a scope written as a
   // literal (`acct = 9007199254740993`) merges neighbours just the same.
   let scoped = false;
   /**
    * The `where:` terms of `source`, each field path read by `at`, which
    * resolves it from the source the walk started at (through the joins
    * that reached `source`).
    */
   const scope = (
      source: unknown,
      depth: number,
      at: (path: readonly string[]) => void,
      /** `at` resolves from `source` itself, through no join. */
      own: boolean,
   ): void => {
      if (depth > 32 || source === null || typeof source !== "object") return;
      const s = source as {
         filterList?: unknown;
         type?: unknown;
         sources?: unknown;
         fields?: unknown;
         query?: { structRef?: unknown; pipeline?: unknown };
      };
      for (const f of Array.isArray(s.filterList) ? s.filterList : []) {
         scoped = true;
         for (const p of fieldPathsIn((f as { e?: unknown }).e)) at(p);
      }
      if (s.type === "composite" && Array.isArray(s.sources)) {
         // A member's terms read the member's own fields; reached through a
         // join, they are read through that join's path.
         for (const m of s.sources) {
            scope(m, depth + 1, own ? (p) => resolve(m, p, []) : at, own);
         }
      }
      // A joined source's own `where:` filters the rows the join reaches.
      for (const f of (Array.isArray(s.fields)
         ? s.fields
         : []) as FieldLike[]) {
         if (f.join === undefined) continue;
         const name = activeName(f);
         scope(f, depth + 1, (p) => at([name, ...p]), false);
      }
      const query = s.query;
      if (query === undefined) return;
      const ref = query.structRef;
      const base =
         typeof ref === "string"
            ? (modelDef as { contents?: Record<string, unknown> } | undefined)
                 ?.contents?.[ref]
            : ref;
      const atBase = (p: readonly string[]) => {
         if (base === undefined || base === null) unresolved = true;
         else resolve(base, p, []);
      };
      if (base === undefined || base === null) unresolved = true;
      else scope(base, depth + 1, atBase, true);
      const stages = Array.isArray(query.pipeline) ? query.pipeline : [];
      stages.forEach((stage, i) => {
         const where = (stage as { filterList?: unknown }).filterList;
         for (const f of Array.isArray(where) ? where : []) {
            scoped = true;
            for (const p of fieldPathsIn((f as { e?: unknown }).e)) {
               if (i === 0) atBase(p);
               else if (p.length === 1) {
                  throughStages(source, p[0], [], [], i - 1);
               } else unresolved = true;
            }
         }
      });
   };
   scope(struct, 0, (p) => resolve(struct, p, []), true);
   return { names, columns, unresolved, scoped, gated: exprs.length > 0 };
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
         : gateReads(struct, exprs, prepared.modelDef);
   } catch {
      return undefined;
   }
}
