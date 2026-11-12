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
 * And the one base edge a source is built from ({@link sourceBaseEdge}),
 * which the source map walks: a rename, an `extend`, a query's base, and the
 * base of an extension of a table-backed source, for which Malloy records no
 * edge ({@link inferredExtendedSource}).
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

// ---------------------------------------------------------------------------
// The base edge: the one source a declaration is built from
// ---------------------------------------------------------------------------

/** The compiled source shape the base edge reads: compiler-set IR fields only. */
interface BaseEdgeDef {
   type?: unknown;
   sourceID?: unknown;
   referenceID?: unknown;
   extends?: unknown;
   connection?: unknown;
   tablePath?: unknown;
   location?: {
      url?: unknown;
      range?: { start?: { line?: unknown; character?: unknown } };
   };
   annotations?: unknown;
   fields?: unknown;
   filterList?: unknown;
   query?: { structRef?: unknown };
}

/** The model shape the base edge resolves ids in. */
interface BaseEdgeModel {
   contents?: Record<string, unknown>;
   sourceRegistry?: Record<
      string,
      { entry?: { type?: string; name?: string } & BaseEdgeDef }
   >;
   imports?: { importURL?: unknown; location?: { url?: unknown } }[];
}

/**
 * How a source reaches its base: `rename` (`r is s`, by `referenceID`),
 * `extends` (`e is s extend { … }`), or `query` (`q is s -> { … }`).
 */
export type SourceBaseHop = "rename" | "extends" | "query";

/** A source's base edge (see {@link sourceBaseEdge}). */
export interface SourceBaseEdge {
   hop: SourceBaseHop;
   /** The base's sourceID, when the hop names one. */
   sourceID: string | undefined;
   /** The base's definition, when it resolves in this model. */
   def: unknown;
}

/** A model's definition for a sourceID, through its source registry. */
function registryDef(
   model: BaseEdgeModel | undefined,
   id: string,
): BaseEdgeDef | undefined {
   const entry = model?.sourceRegistry?.[id]?.entry;
   if (!entry) return undefined;
   if (entry.type === "source_registry_reference" && entry.name) {
      return model?.contents?.[entry.name] as BaseEdgeDef | undefined;
   }
   return entry;
}

/** The sourceID a `structRef` names: itself, or an embedded definition's. */
function structRefId(structRef: unknown): string | undefined {
   if (typeof structRef === "string") return structRef;
   const id = (structRef as { sourceID?: unknown } | null)?.sourceID;
   return typeof id === "string" && id.length > 0 ? id : undefined;
}

/** A query-shaped source's base: a name in the model, a sourceID, or embedded. */
function queryBaseDef(
   def: BaseEdgeDef,
   model: BaseEdgeModel | undefined,
): BaseEdgeDef | undefined {
   if (def.type !== "query_source") return undefined;
   const ref = def.query?.structRef;
   if (typeof ref !== "string") {
      return ref && typeof ref === "object" ? (ref as BaseEdgeDef) : undefined;
   }
   const byName = model?.contents?.[ref] as BaseEdgeDef | undefined;
   if (byName && typeof byName === "object") return byName;
   return registryDef(model, ref);
}

/**
 * The one source `def` is built from, and the hop that reaches it, or
 * undefined for a table, SQL source or composite. Read off compiler-set IR:
 *
 *  - a rename (`r is s`) records `referenceID: s`, read first: Malloy copies
 *    its referent's own `extends` onto a rename, so a rename's `extends`
 *    names its referent's base, the grandparent;
 *  - an extension of a query or SQL source records `extends`;
 *  - an extension of a table-backed source records neither, and its base is
 *    read from what the compiler shares and copies
 *    ({@link inferredExtendedSource});
 *  - a query-shaped source's base is `query.structRef`.
 */
export function sourceBaseEdge(
   def: unknown,
   modelDef: unknown,
): SourceBaseEdge | undefined {
   const d = def as BaseEdgeDef | undefined;
   const model = modelDef as BaseEdgeModel | undefined;
   const own = typeof d?.sourceID === "string" ? d.sourceID : undefined;
   if (typeof d?.referenceID === "string" && d.referenceID !== own) {
      return {
         hop: "rename",
         sourceID: d.referenceID,
         def: registryDef(model, d.referenceID),
      };
   }
   if (typeof d?.extends === "string") {
      return {
         hop: "extends",
         sourceID: d.extends,
         def: registryDef(model, d.extends),
      };
   }
   if (d?.type === "query_source") {
      return {
         hop: "query",
         sourceID: structRefId(d.query?.structRef),
         def: queryBaseDef(d, model),
      };
   }
   const inferred = inferredExtendedSource(def, modelDef) as
      | BaseEdgeDef
      | undefined;
   return inferred
      ? {
           hop: "extends",
           sourceID: inferred.sourceID as string,
           def: inferred,
        }
      : undefined;
}

/**
 * The next definition up a chain: each hop of {@link sourceBaseEdge} tried in
 * turn when the one before does not resolve in this model, so a rename whose
 * referent the registry lacks still reaches the base its copied `extends`
 * names.
 */
export function nextSourceInChain(def: unknown, modelDef: unknown): unknown {
   const d = def as BaseEdgeDef | undefined;
   const model = modelDef as BaseEdgeModel | undefined;
   if (!d || typeof d !== "object") return undefined;
   const own = typeof d.sourceID === "string" ? d.sourceID : undefined;
   return (
      (typeof d.referenceID === "string" && d.referenceID !== own
         ? registryDef(model, d.referenceID)
         : undefined) ??
      (typeof d.extends === "string"
         ? registryDef(model, d.extends)
         : undefined) ??
      queryBaseDef(d, model) ??
      inferredExtendedSource(def, modelDef)
   );
}

/** Per compiled model, each table-backed source's inferred base (memo). */
const inferredMemo = new WeakMap<object, Map<unknown, BaseEdgeDef | null>>();

/** Per field object, the signature it is compared by. */
const fieldSignatures = new WeakMap<object, string>();

/**
 * A field's identity across an extension, which copies it: its whole compiled
 * definition, with a joined source's own fields summarised by name (a join
 * field embeds the joined source).
 */
function fieldSignature(field: unknown): string {
   if (!field || typeof field !== "object") return String(field);
   const cached = fieldSignatures.get(field);
   if (cached !== undefined) return cached;
   const signature = JSON.stringify(
      field,
      function (this: unknown, key: string, value: unknown) {
         if (key === "fields" && this !== field && Array.isArray(value)) {
            return value.map((f) => fieldKeyOf(f) ?? null);
         }
         return value;
      },
   );
   fieldSignatures.set(field, signature);
   return signature;
}

function fieldKeyOf(field: unknown): string | undefined {
   const f = field as { as?: unknown; name?: unknown } | undefined;
   return typeof f?.as === "string"
      ? f.as
      : typeof f?.name === "string"
        ? f.name
        : undefined;
}

/** Per definition, its fields' signatures by name. */
const definitionSignatures = new WeakMap<object, Map<string, string>>();

/** Each field's signature, by name. */
function signaturesByName(def: BaseEdgeDef): Map<string, string> {
   const cached = definitionSignatures.get(def);
   if (cached) return cached;
   const out = new Map<string, string>();
   for (const f of Array.isArray(def.fields) ? def.fields : []) {
      const key = fieldKeyOf(f);
      if (key !== undefined) out.set(key, fieldSignature(f));
   }
   definitionSignatures.set(def, out);
   return out;
}

/** Whether `base`'s filters open `ext`'s, in order (an extension appends). */
function filtersPrefix(base: BaseEdgeDef, ext: BaseEdgeDef): boolean {
   const b = Array.isArray(base.filterList) ? base.filterList : [];
   const e = Array.isArray(ext.filterList) ? ext.filterList : [];
   if (b.length > e.length) return false;
   return b.every(
      (f, i) => f === e[i] || JSON.stringify(f) === JSON.stringify(e[i]),
   );
}

/** Per field object, its signature without the parts a `rename:` sets. */
const renamedSignatures = new WeakMap<object, string>();

/**
 * A field's signature as a `rename:` leaves it: a rename copies the field
 * under a new name (`as`), at the rename's own location, with its references
 * recorded there, and changes nothing else.
 */
function renameSignature(field: unknown): string {
   if (!field || typeof field !== "object") return String(field);
   const cached = renamedSignatures.get(field);
   if (cached !== undefined) return cached;
   const {
      as: _as,
      location: _location,
      refSummary: _refSummary,
      ...rest
   } = field as Record<string, unknown>;
   const signature = fieldSignature(rest);
   renamedSignatures.set(field, signature);
   return signature;
}

/**
 * `a` is at most `b`: its filters open `b`'s, and each of its fields is in `b`
 * unchanged, or renamed (`rename: n is f` keeps `f` as `name` and sets `as`).
 */
function atMost(a: BaseEdgeDef, b: BaseEdgeDef): boolean {
   if (!filtersPrefix(a, b)) return false;
   const theirs = signaturesByName(b);
   const renamed = new Map<string, unknown>();
   for (const f of Array.isArray(b.fields) ? b.fields : []) {
      const g = f as { name?: unknown; as?: unknown };
      if (typeof g.as === "string" && typeof g.name === "string") {
         renamed.set(g.name, f);
      }
   }
   for (const f of Array.isArray(a.fields) ? a.fields : []) {
      const name = fieldKeyOf(f);
      if (name === undefined) continue;
      if (theirs.get(name) === fieldSignature(f)) continue;
      const own = (f as { as?: unknown }).as;
      const copy = own === undefined ? renamed.get(name) : undefined;
      if (copy === undefined || renameSignature(copy) !== renameSignature(f)) {
         return false;
      }
   }
   return true;
}

/** Where a source's own statement starts, when the compiler recorded it. */
function startOf(
   def: BaseEdgeDef,
): { url: string; line: number; character: number } | undefined {
   const url = def.location?.url;
   const start = def.location?.range?.start;
   return typeof url === "string" &&
      typeof start?.line === "number" &&
      typeof start.character === "number"
      ? { url, line: start.line, character: start.character }
      : undefined;
}

/**
 * Where `other` is declared relative to `ext`: earlier in the same file or in
 * a file `ext`'s file imports (`before`), later or in a file importing it
 * (`after`), or `unknown`. `ModelDef.imports` lists the compiled model's own
 * imports, not its imports' imports, so an order it does not show is unknown,
 * never assumed.
 */
function orderOf(
   other: BaseEdgeDef,
   ext: BaseEdgeDef,
   model: BaseEdgeModel | undefined,
): "before" | "after" | "unknown" {
   const o = startOf(other);
   const e = startOf(ext);
   if (!o || !e) return "unknown";
   if (o.url === e.url) {
      return o.line < e.line || (o.line === e.line && o.character < e.character)
         ? "before"
         : "after";
   }
   const imports = (from: string, to: string) =>
      (model?.imports ?? []).some(
         (i) => i.location?.url === from && i.importURL === to,
      );
   if (imports(e.url, o.url)) return "before";
   if (imports(o.url, e.url)) return "after";
   return "unknown";
}

/** Per compiled model, its source definitions. */
const sourcesMemo = new WeakMap<object, BaseEdgeDef[]>();

/** Every source definition a model holds, once each. */
function modelSources(model: BaseEdgeModel | undefined): BaseEdgeDef[] {
   const cached = model ? sourcesMemo.get(model) : undefined;
   if (cached) return cached;
   const found = collectSources(model);
   if (model) sourcesMemo.set(model, found);
   return found;
}

function collectSources(model: BaseEdgeModel | undefined): BaseEdgeDef[] {
   const out = new Set<BaseEdgeDef>();
   for (const value of Object.values(model?.contents ?? {})) {
      const d = value as BaseEdgeDef;
      if (d && typeof d === "object" && typeof d.sourceID === "string") {
         out.add(d);
      }
   }
   for (const id of Object.keys(model?.sourceRegistry ?? {})) {
      const d = registryDef(model, id);
      if (d && typeof d.sourceID === "string") out.add(d);
   }
   return [...out];
}

/**
 * The base of an extension of a table-backed source, for which Malloy records
 * no `extends` and no `referenceID` (it sets `extends` only on a query or SQL
 * source), read from what the compiler shares and copies, never from text. A
 * candidate base:
 *
 *  - holds the annotations object the extension holds (an extension that
 *    declares no annotation shares its base's), or the one the extension's
 *    own `inherits` (one that declares an annotation gets a fresh object over
 *    its base's; see declared_notes.ts);
 *  - reads the same table on the same connection;
 *  - has every field of its own in the extension, unchanged (an extension
 *    copies its base's fields, access modifiers included, and adds its own);
 *  - has filters that open the extension's (an extension appends its own);
 *  - is declared earlier in the file, or in a file this one imports.
 *
 * The base is the candidate every other is at most. When several are alike
 * (a source and its rename, or an empty `extend {}` of it), it is the one the
 * others are built from, which they add nothing to. Two candidates neither of
 * which holds the other give no edge: an unresolved base is the answer before
 * this reader, never a wrong one. Undefined for any other source.
 */
export function inferredExtendedSource(
   def: unknown,
   modelDef: unknown,
): unknown {
   const ext = def as BaseEdgeDef | undefined;
   if (!ext || typeof ext !== "object" || ext.type !== "table") return;
   const own = typeof ext.sourceID === "string" ? ext.sourceID : undefined;
   if (own === undefined || typeof ext.extends === "string") return;
   if (typeof ext.referenceID === "string" && ext.referenceID !== own) return;
   const model = modelDef as BaseEdgeModel | undefined;
   const memoKey =
      modelDef && typeof modelDef === "object" ? modelDef : undefined;
   let memo = memoKey ? inferredMemo.get(memoKey) : undefined;
   if (memoKey && !memo) {
      memo = new Map();
      inferredMemo.set(memoKey, memo);
   }
   if (memo?.has(def)) return memo.get(def) ?? undefined;
   // Bases are declared strictly earlier, so the walk ends; a cycle reads as
   // no edge all the same.
   memo?.set(def, null);
   const found = inferBase(ext, model);
   memo?.set(def, found ?? null);
   return found;
}

function inferBase(
   ext: BaseEdgeDef,
   model: BaseEdgeModel | undefined,
): BaseEdgeDef | undefined {
   const sameTable = modelSources(model)
      .filter(
         (b) =>
            b !== ext &&
            b.sourceID !== ext.sourceID &&
            b.type === "table" &&
            b.connection === ext.connection &&
            b.tablePath === ext.tablePath,
      )
      .map((b) => ({ def: b, order: orderOf(b, ext, model) }))
      .filter((b) => b.order !== "after");
   // An extension sharing its annotations object with an earlier source
   // declared none, and its base is among those sharers. Otherwise it holds a
   // fresh object of its own, whose `inherits` is its base's. (An unannotated
   // extension's `inherits` is its grandparent's, so the sharers decide first.)
   const sharers = sameTable.filter(
      (b) => b.def.annotations === ext.annotations,
   );
   const own = ext.annotations && typeof ext.annotations === "object";
   const inherits = own
      ? (ext.annotations as { inherits?: unknown }).inherits
      : undefined;
   const pool = (
      sharers.length > 0
         ? sharers
         : own
           ? sameTable.filter((b) => b.def.annotations === inherits)
           : []
   ).filter((b) => atMost(b.def, ext));
   // A source that would qualify but whose order is unknown might be the
   // base, or might make this extension's object its own: no edge.
   if (pool.some((b) => b.order === "unknown")) return undefined;
   const candidates = pool.map((b) => b.def);
   if (candidates.length === 0) return undefined;
   // The candidates no other candidate strictly holds.
   const maximal = candidates.filter(
      (c) => !candidates.some((o) => o !== c && atMost(c, o) && !atMost(o, c)),
   );
   if (maximal.length === 1) return maximal[0];
   // Alike candidates (each at most every other): the one the others are
   // built from, when there is exactly one.
   const alike = maximal.every((a) => maximal.every((b) => atMost(a, b)));
   if (!alike) return undefined;
   const roots = maximal.filter((c) =>
      maximal.every((o) => o === c || chainReaches(o, c, model)),
   );
   return roots.length === 1 ? roots[0] : undefined;
}

/** Whether `from`'s chain reaches `to`. */
function chainReaches(
   from: BaseEdgeDef,
   to: BaseEdgeDef,
   model: BaseEdgeModel | undefined,
): boolean {
   const seen = new Set<unknown>();
   let current: unknown = from;
   for (let hops = 0; current && hops < 64; hops++) {
      if (current === to) return true;
      if (seen.has(current)) return false;
      seen.add(current);
      current = nextSourceInChain(current, model);
   }
   return false;
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
