// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The source graph governance walks: the derivation hops from one struct to
 * the structs it was built from. Three hops, read off compiler-set IR only:
 *
 *  - the declaration a struct derives from, via `ModelDef.sourceRegistry`
 *    ({@link resolveDeclaredSource});
 *  - a `query_source`'s base, via `query.structRef`
 *    ({@link resolveQuerySourceBase});
 *  - a `query_source`'s resolved composite member, via
 *    `query.compositeResolvedSourceDef` ({@link resolveCompositeResolvedBase}).
 *
 * Moved verbatim from `gate_registry_walk.ts`. Bundled into the package-load
 * worker: keep imports to `@malloydata/malloy` types.
 */

import { isSourceDef, type ModelDef, type SourceDef } from "@malloydata/malloy";

/**
 * The declared source a struct was created from, via `ModelDef.sourceRegistry`
 * (`referenceID` first, then `sourceID`).
 *
 * Collapsing the last two outcomes would fail open:
 *  - `resolved`: the declaration this struct derives from.
 *  - `none`: nothing to follow (no id, no registry, or the entry is `struct`
 *    itself). The common case for a top-level source; means "no gate here".
 *  - `unresolvable`: an entry exists but does not yield a usable `SourceDef`.
 *    The base exists and cannot be read, so the caller denies.
 */
export function resolveDeclaredSource(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
):
   | { kind: "resolved"; source: SourceDef }
   | { kind: "none" }
   | { kind: "unresolvable" } {
   if (!modelDef) return { kind: "none" };
   let sawBrokenEntry = false;
   for (const id of [struct.referenceID, struct.sourceID]) {
      const entry = id ? modelDef.sourceRegistry?.[id]?.entry : undefined;
      if (!entry) continue;
      const declared =
         entry.type === "source_registry_reference"
            ? modelDef.contents[entry.name]
            : entry;
      // Its own declaration — nothing to inherit from, and not a failure.
      if (declared === struct) continue;
      if (!declared || !isSourceDef(declared)) {
         sawBrokenEntry = true;
         continue;
      }
      return { kind: "resolved", source: declared };
   }
   return sawBrokenEntry ? { kind: "unresolvable" } : { kind: "none" };
}

/**
 * The `SourceDef` a `query_source` (`Z is X -> {...}`) derives from, via
 * `query.structRef`; `undefined` if `struct` is not a `query_source` or the
 * ref does not resolve to a `SourceDef`.
 *
 * Duck-typed because `QuerySourceDef` is not exported from the package root.
 */
export function resolveQuerySourceBase(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
): SourceDef | undefined {
   const duck = struct as unknown as {
      type: string;
      query?: { structRef?: SourceDef | string };
   };
   if (duck.type !== "query_source") return undefined;
   const ref = duck.query?.structRef;
   const base = typeof ref === "string" ? modelDef?.contents[ref] : ref;
   return base && isSourceDef(base) ? base : undefined;
}

/**
 * For a `query_source` whose base is a composite (`qs is compose(a, b) ->
 * {...}`), the member Malloy resolved for this derivation. It is carried on
 * `query.compositeResolvedSourceDef`, not `query.structRef` (the raw
 * composite).
 */
export function resolveCompositeResolvedBase(
   struct: SourceDef,
): SourceDef | undefined {
   const duck = struct as unknown as {
      type: string;
      query?: { compositeResolvedSourceDef?: SourceDef };
   };
   return duck.type === "query_source"
      ? duck.query?.compositeResolvedSourceDef
      : undefined;
}

/**
 * Every struct reachable from `roots` (excluded) by the two hops
 * {@link effectiveAncestorGateExprs} takes, transitively.
 *
 * `modelDef.contents` plus `sourceRegistry` does not cover everything the
 * gate walks reach: an imported `query_source`'s base arrives inline on
 * `query.structRef`, and a composite's resolved member lives in neither. A
 * sweep asking "is there a gate anywhere the walks look" must follow both or
 * it answers `false` for a gated model. Identity-guarded against cycles.
 */
export function derivedStructsReachable(
   roots: readonly SourceDef[],
   modelDef: ModelDef | undefined,
): SourceDef[] {
   const seen = new Set<SourceDef>(roots);
   const found: SourceDef[] = [];
   const worklist: SourceDef[] = [...roots];
   for (let i = 0; i < worklist.length; i++) {
      const struct = worklist[i];
      for (const next of [
         resolveQuerySourceBase(struct, modelDef),
         resolveCompositeResolvedBase(struct),
      ]) {
         if (!next || seen.has(next)) continue;
         seen.add(next);
         found.push(next);
         worklist.push(next);
      }
   }
   return found;
}

/**
 * The source `struct` extends (`source: b is a extend { … }` records
 * `extends: "a@url"`), resolved through the source registry; undefined for a
 * source that extends nothing or whose base the registry does not hold.
 */
export function resolveExtendedSource(
   struct: SourceDef,
   modelDef: ModelDef | undefined,
): SourceDef | undefined {
   const id = (struct as unknown as { extends?: unknown }).extends;
   if (typeof id !== "string" || !modelDef) return undefined;
   const entry = modelDef.sourceRegistry?.[id]?.entry;
   if (!entry) return undefined;
   const declared =
      entry.type === "source_registry_reference"
         ? modelDef.contents[entry.name]
         : entry;
   return declared && isSourceDef(declared) && declared !== struct
      ? declared
      : undefined;
}

/**
 * `roots` and every source their walks reach: the declaration each names
 * ({@link resolveDeclaredSource}) and the source each extends
 * ({@link resolveExtendedSource}), as well as what
 * {@link derivedStructsReachable} follows. For a reader asking which sources a
 * set of entry points reads, not which gates they inherit. Identity-guarded
 * against cycles.
 */
export function structsReachedFrom(
   roots: readonly SourceDef[],
   modelDef: ModelDef | undefined,
): SourceDef[] {
   const seen = new Set<SourceDef>(roots);
   const worklist: SourceDef[] = [...roots];
   for (let i = 0; i < worklist.length; i++) {
      const struct = worklist[i];
      const declared = resolveDeclaredSource(struct, modelDef);
      for (const next of [
         declared.kind === "resolved" ? declared.source : undefined,
         resolveExtendedSource(struct, modelDef),
         resolveQuerySourceBase(struct, modelDef),
         resolveCompositeResolvedBase(struct),
      ]) {
         if (!next || seen.has(next)) continue;
         seen.add(next);
         worklist.push(next);
      }
   }
   return worklist;
}

/** A join a source declares, as its compiled field records it. */
export interface SourceJoin {
   /** The join's name on the source (`carriers` in `join_one: carriers …`). */
   name: string;
   /** `one`, `many` or `cross`. */
   relationship: "one" | "many" | "cross";
   /** The connection the joined source reads. */
   connection: string | undefined;
   /** The joined source's declaration, when it is a declared source. */
   sourceID: string | undefined;
   /** The joined source's primary key, when it has one. */
   primaryKey: string | undefined;
   /**
    * The joined side's column the join is keyed on, when its condition is one
    * equality between a field of the joined source and an expression of the
    * joining one (`with` and the plain `on j.k = fk` both compile to it);
    * undefined for any other condition.
    */
   keyColumn: string | undefined;
   /**
    * The source's other joins its condition reads, directly or through the
    * source's fields (`join_one: c on c.id = s.carrier` reads `s`), and
    * whether it reaches past one into a join of a joined source; undefined
    * when the compiler did not record what a field it reads uses. A join
    * whose condition reads a join_many is reached through that join_many.
    */
   condition: FieldJoinReads | undefined;
   /** The joined source's own fields, read-only. */
   fields: unknown[];
}

/** What one field's value reads through joins (see {@link fieldJoinUsage}). */
export interface FieldJoinReads {
   /** The source's own joins it reads, by name. */
   joins: ReadonlySet<string>;
   /** Whether it also reads through a join of a joined source (two hops or more). */
   nested: boolean;
}

interface IrField {
   name?: unknown;
   as?: unknown;
   join?: unknown;
   e?: unknown;
   connection?: unknown;
   sourceID?: unknown;
   primaryKey?: unknown;
   onExpression?: unknown;
   fields?: unknown;
   refSummary?: { fieldUsage?: { path?: unknown }[] };
}

interface IrExpr {
   node?: unknown;
   path?: unknown;
   kids?: { left?: IrExpr; right?: IrExpr };
}

function fieldName(f: IrField): string | undefined {
   return typeof f.as === "string"
      ? f.as
      : typeof f.name === "string"
        ? f.name
        : undefined;
}

function isJoin(f: IrField | undefined): boolean {
   return f?.join === "one" || f?.join === "many" || f?.join === "cross";
}

/** Every field path an expression reads (`{node: "field", path}`), in order. */
function expressionPaths(e: unknown, out: unknown[][] = []): unknown[][] {
   if (Array.isArray(e)) {
      for (const k of e) expressionPaths(k, out);
   } else if (e && typeof e === "object") {
      const node = e as IrExpr;
      if (node.node === "field" && Array.isArray(node.path)) {
         out.push(node.path as unknown[]);
      }
      for (const v of Object.values(e)) {
         if (v && typeof v === "object") expressionPaths(v, out);
      }
   }
   return out;
}

/**
 * The one walk behind {@link sourceJoins} and {@link fieldJoinUsage}: what a
 * field of `def`, or a join's condition, reads through the source's joins.
 */
function joinReader(def: unknown) {
   const fields = ((def as { fields?: unknown[] } | undefined)?.fields ??
      []) as IrField[];
   const byName = new Map<string, IrField>();
   for (const f of fields) {
      const n = fieldName(f);
      if (n !== undefined) byName.set(n, f);
   }
   const memo = new Map<string, FieldJoinReads | undefined>();
   const joinedMemo = new Map<
      string,
      Map<string, FieldJoinReads | undefined>
   >();
   const stack = new Set<string>();

   /** Fold the reads of `paths` (relative to `def`) into one result. */
   const readsOf = (
      self: string,
      paths: readonly unknown[][],
   ): FieldJoinReads | undefined => {
      const joins = new Set<string>();
      let nested = false;
      for (const path of paths) {
         const head = path[0];
         if (typeof head !== "string" || head === self) continue;
         const target = byName.get(head);
         if (isJoin(target)) {
            // The join itself, and what its own condition reads.
            const via = visit(head);
            if (via === undefined) return undefined;
            for (const j of via.joins) joins.add(j);
            nested ||= via.nested;
            if (path.length > 2) {
               nested = true;
            } else if (typeof path[1] === "string") {
               // The joined field may read a join of its own source.
               let inner = joinedMemo.get(head);
               if (!inner) {
                  inner = fieldJoinUsage(target);
                  joinedMemo.set(head, inner);
               }
               const reads = inner.get(path[1]);
               if (reads === undefined && inner.has(path[1])) nested = true;
               else if (reads && (reads.joins.size > 0 || reads.nested)) {
                  nested = true;
               }
            }
            continue;
         }
         const reads = visit(head);
         if (reads === undefined) return undefined;
         for (const j of reads.joins) joins.add(j);
         nested ||= reads.nested;
      }
      return { joins, nested };
   };

   /** A join's condition: the other joins it reads (not the join itself). */
   const conditionOf = (name: string): FieldJoinReads | undefined => {
      const f = byName.get(name);
      if (!f || f.onExpression === undefined) {
         return { joins: new Set(), nested: false };
      }
      const paths = expressionPaths(f.onExpression);
      return readsOf(name, paths);
   };

   const visit = (name: string): FieldJoinReads | undefined => {
      if (memo.has(name)) return memo.get(name);
      const f = byName.get(name);
      if (!f) return { joins: new Set(), nested: false };
      if (stack.has(name)) return undefined;
      stack.add(name);
      let result: FieldJoinReads | undefined;
      if (isJoin(f)) {
         const condition = conditionOf(name);
         result = condition && {
            joins: new Set([name, ...condition.joins]),
            nested: condition.nested,
         };
      } else if (f.e === undefined) {
         // A column read from the table has no expression and reads no join.
         result = { joins: new Set(), nested: false };
      } else {
         const usage = f.refSummary?.fieldUsage;
         result = Array.isArray(usage)
            ? readsOf(
                 name,
                 usage.map((u) =>
                    Array.isArray(u.path) ? (u.path as unknown[]) : [],
                 ),
              )
            : undefined;
      }
      stack.delete(name);
      memo.set(name, result);
      return result;
   };

   return { fields, visit, conditionOf };
}

/** The joins `def` declares or inherits, in field order. */
export function sourceJoins(def: unknown): SourceJoin[] {
   const reader = joinReader(def);
   const out: SourceJoin[] = [];
   for (const f of reader.fields) {
      if (!isJoin(f)) continue;
      const name = fieldName(f);
      if (!name) continue;
      out.push({
         name,
         relationship: f.join as SourceJoin["relationship"],
         connection:
            typeof f.connection === "string" ? f.connection : undefined,
         sourceID: typeof f.sourceID === "string" ? f.sourceID : undefined,
         primaryKey:
            typeof f.primaryKey === "string" ? f.primaryKey : undefined,
         keyColumn: joinKeyColumn(name, f.onExpression),
         condition: reader.conditionOf(name),
         fields: Array.isArray(f.fields) ? f.fields : [],
      });
   }
   return out;
}

/** See {@link SourceJoin.keyColumn}. */
function joinKeyColumn(join: string, on: unknown): string | undefined {
   const expr = on as IrExpr | undefined;
   if (expr?.node !== "=") return undefined;
   const sides = [expr.kids?.left, expr.kids?.right];
   const isJoined = (e: IrExpr | undefined) =>
      e?.node === "field" &&
      Array.isArray(e.path) &&
      e.path.length === 2 &&
      e.path[0] === join &&
      typeof e.path[1] === "string";
   const joined = sides.filter(isJoined);
   if (joined.length !== 1) return undefined;
   const other = sides.find((s) => s !== joined[0]);
   if (expressionPaths(other).some((p) => p[0] === join)) return undefined;
   return (joined[0]?.path as string[])[1];
}

/**
 * For each field of `def` that is not itself a join, the joins its value reads,
 * directly or through other fields of `def` (`dimension: a is carriers.name`
 * reads `carriers`; `dimension: b is upper(a)` reads it too), including the
 * joins a read join's condition reads (`c` keyed on `s.carrier` reads `s`), and
 * whether it reaches past one of them into a join of the joined source. A
 * field whose usage the compiler did not record reads `undefined`: unknown,
 * so a caller treats it as reading every join.
 */
export function fieldJoinUsage(
   def: unknown,
): Map<string, FieldJoinReads | undefined> {
   const reader = joinReader(def);
   const out = new Map<string, FieldJoinReads | undefined>();
   for (const f of reader.fields) {
      const n = fieldName(f);
      if (n === undefined || isJoin(f)) continue;
      out.set(n, reader.visit(n));
   }
   return out;
}

/**
 * The joined source of `join`, read as {@link sourceJoins} and
 * {@link fieldJoinUsage} read a declared source: its joins, and what each of
 * its fields reads through them.
 */
export function joinedSourceReads(join: SourceJoin): {
   joins: SourceJoin[];
   fieldJoins: Map<string, FieldJoinReads | undefined>;
} {
   const def = { fields: join.fields };
   return { joins: sourceJoins(def), fieldJoins: fieldJoinUsage(def) };
}
