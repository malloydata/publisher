// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The source map: one read-only description of a model's sources, and the one
 * place materialization walks a source's chain (materialization v2, phase 3;
 * design in `docs/materialization-source-map.md`).
 *
 * Every reader that used to walk the compiled IR on its own (the build-side
 * reach, the path report's chain, the unreachable verdict's reachability)
 * reads it instead. It records structure only: no content address (the build
 * plan's), no Malloy text (the rewriter's and generator's), and no IR edits. It
 * reads the chain through the governance IR's hop readers
 * (`governance/ir/source_graph.ts`), shared with the gate walks.
 *
 * The keying rule: a declaration is identified by its `sourceID`, a stored
 * table by `(sourceEntityId, destination)`, and a name is for display and logs
 * only. A `sourceID` survives import and re-export, so a source an
 * `index.malloy` re-exports is one node in every model that can see it.
 */

import { isSourceDef, type ModelDef, type SourceDef } from "@malloydata/malloy";
import {
   fieldJoinUsage,
   filterJoinReads,
   inferredExtendedSource,
   joinedSourceReads,
   nextSourceInChain,
   resolveCompositeResolvedBase,
   resolveDeclaredSource,
   resolveExtendedSource,
   resolveQuerySourceBase,
   sourceBaseEdge,
   sourceJoins,
   type FieldJoinReads,
   type SourceJoin,
} from "../../governance/ir/source_graph";

export type {
   FieldJoinReads,
   SourceJoin,
} from "../../governance/ir/source_graph";

/** The compiled source shape the map reads: compiler-set IR fields only. */
interface MappedSourceDef {
   type?: unknown;
   sourceID?: unknown;
   referenceID?: unknown;
   /** The sourceID of the source this one extends, when it extends one. */
   extends?: unknown;
   /** A query-shaped source's query: `structRef` is the source it reads. */
   query?: {
      structRef?: unknown;
      pipeline?: unknown;
      compositeResolvedSourceDef?: unknown;
   };
   fields?: unknown[];
   /** A composite source's members (`compose(a, b)`), in order. */
   sources?: unknown[];
}

/**
 * How a source reaches the one it is built from. A composite has no single
 * base; its members are on {@link SourceNode.members}. A query over a
 * composite reaches the composite through `query.structRef`, like any query.
 */
type BaseHop = "extends" | "rename" | "query";

/** A source's base edge: the hop, the id it names, and the definition when it resolves. */
interface BaseEdge {
   hop: BaseHop;
   /** The base's sourceID, when the hop names one. */
   sourceID: string | undefined;
   /** The base's definition, when it resolves in this model. */
   def: MappedSourceDef | undefined;
}

/** What kind of source a node is. */
type SourceKind = "table" | "extend" | "rename" | "query" | "composite";

/** One declaration in the map. */
export interface SourceNode {
   /** The declaration's identity. */
   sourceID: string;
   /** The name it has in this model's namespace, when it has one; display only. */
   name: string | undefined;
   kind: SourceKind;
   /** The source it is built from; undefined for a table, SQL source or composite. */
   base: BaseEdge | undefined;
   /** A composite's members' sourceIDs, in order; empty for any other kind. */
   members: string[];
   def: MappedSourceDef;
}

/** A model's minimal view for resolving ids: its contents and source registry. */
interface IrModel {
   contents?: Record<string, unknown>;
   sourceRegistry?: Record<
      string,
      { entry?: { type?: string; name?: string } & MappedSourceDef }
   >;
}

/**
 * The sourceID a `structRef` names: the reference itself, or the `sourceID` of
 * an embedded definition. Undefined for an inline source.
 */
export function structRefSourceId(structRef: unknown): string | undefined {
   if (typeof structRef === "string") return structRef;
   const sourceID = (structRef as { sourceID?: unknown } | null)?.sourceID;
   return typeof sourceID === "string" && sourceID.length > 0
      ? sourceID
      : undefined;
}

/** A model's definition for an id: through its source registry. */
function byId(
   model: IrModel | undefined,
   id: string,
): MappedSourceDef | undefined {
   const entry = model?.sourceRegistry?.[id]?.entry;
   if (!entry) return undefined;
   if (entry.type === "source_registry_reference" && entry.name) {
      return model?.contents?.[entry.name] as MappedSourceDef | undefined;
   }
   return entry;
}

/**
 * The one source `def` is built from, and the hop that reaches it, as the
 * governance IR reads it ({@link sourceBaseEdge}): a rename first (its copied
 * `extends` names the grandparent), then `extends`, then a query-shaped
 * source's base, and for an extension of a table-backed source, which Malloy
 * records no edge for, the base the compiler's shared and copied structure
 * names. Persistence is inherited along the first two hops only; a
 * query-shaped source starts a new segment. A composite has members, not a
 * base (see {@link compositeMemberIds}).
 */
function baseEdgeOf(
   def: MappedSourceDef,
   modelDef: unknown,
): BaseEdge | undefined {
   const edge = sourceBaseEdge(def, modelDef);
   return (
      edge && {
         hop: edge.hop,
         sourceID: edge.sourceID,
         def: edge.def as MappedSourceDef | undefined,
      }
   );
}

/** A composite source's members' sourceIDs, in order. */
function compositeMemberIds(def: MappedSourceDef): string[] {
   if (def.type !== "composite" || !Array.isArray(def.sources)) return [];
   return def.sources.flatMap((member) => {
      const id = structRefSourceId(member);
      return id === undefined ? [] : [id];
   });
}

function kindOf(def: MappedSourceDef, base: BaseEdge | undefined): SourceKind {
   if (def.type === "composite") return "composite";
   if (!base) return "table";
   if (base.hop === "extends") return "extend";
   if (base.hop === "rename") return "rename";
   return "query";
}

/**
 * A model's declarations, by `sourceID`, with a per-model name index. Built
 * once per compiled model (memoized on the `ModelDef`'s identity) from its
 * `contents` and source registry, so an imported declaration is the declaring
 * model's node.
 */
export class SourceMap {
   private static readonly memo = new WeakMap<object, SourceMap>();

   private readonly nodesById = new Map<string, SourceNode>();
   private readonly idsByName = new Map<string, string>();

   private constructor(private readonly modelDef: unknown) {
      const model = modelDef as IrModel | undefined;
      for (const [name, value] of Object.entries(model?.contents ?? {})) {
         const def = value as MappedSourceDef;
         if (typeof def?.sourceID !== "string") continue;
         this.idsByName.set(name, def.sourceID);
         this.add(def, name);
      }
      for (const id of Object.keys(model?.sourceRegistry ?? {})) {
         if (this.nodesById.has(id)) continue;
         const def = byId(model, id);
         if (def && typeof def.sourceID === "string") this.add(def, undefined);
      }
   }

   /** The map of `modelDef`, built once per compiled model. */
   static of(modelDef: unknown): SourceMap {
      if (modelDef === null || typeof modelDef !== "object") {
         return new SourceMap(modelDef);
      }
      let map = SourceMap.memo.get(modelDef);
      if (!map) {
         map = new SourceMap(modelDef);
         SourceMap.memo.set(modelDef, map);
      }
      return map;
   }

   private add(def: MappedSourceDef, name: string | undefined): void {
      const sourceID = def.sourceID as string;
      if (this.nodesById.has(sourceID)) return;
      const base = baseEdgeOf(def, this.modelDef);
      this.nodesById.set(sourceID, {
         sourceID,
         name,
         kind: kindOf(def, base),
         base,
         members: compositeMemberIds(def),
         def,
      });
   }

   /** The compiled model this map was built from. */
   model(): unknown {
      return this.modelDef;
   }

   /** The node for a declaration. */
   node(sourceID: string): SourceNode | undefined {
      return this.nodesById.get(sourceID);
   }

   /** The declaration a name in this model's namespace refers to. */
   sourceIDOf(name: string): string | undefined {
      return this.idsByName.get(name);
   }

   /**
    * The joins a declaration has, by name (its own and those it inherits), as
    * the governance IR reads them. Empty for an unknown declaration.
    */
   joinsOf(sourceID: string): SourceJoin[] {
      const node = this.nodesById.get(sourceID);
      return node ? sourceJoins(node.def) : [];
   }

   /**
    * For each non-join field of a declaration, the joins its value reads
    * (see {@link fieldJoinUsage}); `undefined` for a field whose usage is
    * unknown.
    */
   fieldJoinsOf(sourceID: string): Map<string, FieldJoinReads | undefined> {
      const node = this.nodesById.get(sourceID);
      return node ? fieldJoinUsage(node.def) : new Map();
   }

   /**
    * The joins a declaration's own `where:` reads (see {@link filterJoinReads});
    * undefined when unknown.
    */
   filterJoinsOf(sourceID: string): FieldJoinReads | undefined {
      const node = this.nodesById.get(sourceID);
      return node ? filterJoinReads(node.def) : undefined;
   }

   /**
    * The joined side of one of a declaration's joins, as the join holds it:
    * its own joins and what each of its fields reads through them.
    */
   joinedReadsOf(join: SourceJoin): {
      joins: SourceJoin[];
      fieldJoins: Map<string, FieldJoinReads | undefined>;
   } {
      return joinedSourceReads(join);
   }

   /** Every node, declaration order first, then imports the registry holds. */
   nodes(): SourceNode[] {
      return [...this.nodesById.values()];
   }

   /**
    * The `sourceID`s `struct` derives from, itself first: through `extends`, a
    * rename and a query-shaped source's base. Persistence is inherited along
    * the first two, so a table on any of them can answer a query on the source;
    * a query-shaped base is where a stored table stands for everything below.
    * A hop that resolves to nothing still names the source it points at.
    */
   chainOf(struct: unknown): string[] {
      const out: string[] = [];
      const seen = new Set<unknown>();
      let current = struct as MappedSourceDef | undefined;
      for (let hops = 0; current && hops < 64; hops++) {
         if (seen.has(current)) break;
         seen.add(current);
         const id = current.sourceID;
         if (typeof id === "string" && !out.includes(id)) out.push(id);
         // Each hop falls through to the next when it does not resolve here,
         // so an `extends` the registry lacks still reaches a rename or base.
         const next = nextSourceInChain(current, this.modelDef) as
            | MappedSourceDef
            | undefined;
         if (!next) {
            for (const ref of [current.referenceID, current.extends]) {
               if (typeof ref === "string" && !out.includes(ref)) out.push(ref);
            }
         }
         current = next;
      }
      return out;
   }
}

/** The `sourceID`s `struct` derives from, itself first (see {@link SourceMap.chainOf}). */
export function sourceChain(struct: unknown, modelDef: unknown): string[] {
   return SourceMap.of(modelDef).chainOf(struct);
}

// ---------------------------------------------------------------------------
// Pipeline references
// ---------------------------------------------------------------------------

/** A pipeline nested deeper than this is treated as reaching an unknown source. */
const MAX_PIPELINE_DEPTH = 200;

/**
 * One reference a query pipeline makes. `sourceID` is the referenced source's
 * id when it is named; `ref` is what a walk follows: the sourceID of a named
 * join, else the join or stage `structRef` as written (an id or an embedded
 * definition).
 */
interface PipelineReference {
   sourceID: string | undefined;
   ref: unknown;
}

/**
 * Every reference a query pipeline makes: each stage's `structRef` and each
 * join. Named sources are not descended into; the caller looks them up.
 *
 * Undefined when the pipeline nests past {@link MAX_PIPELINE_DEPTH}, which a
 * caller must treat as an unknown reference.
 */
export function pipelineReferences(
   pipeline: unknown,
): PipelineReference[] | undefined {
   const out: PipelineReference[] = [];
   let tooDeep = false;
   const walk = (node: unknown, depth: number): void => {
      if (tooDeep) return;
      if (depth > MAX_PIPELINE_DEPTH) {
         tooDeep = true;
         return;
      }
      if (node === null || typeof node !== "object") return;
      if (Array.isArray(node)) {
         for (const item of node) walk(item, depth + 1);
         return;
      }
      const record = node as Record<string, unknown>;
      if (typeof record.join === "string") {
         const sourceID =
            typeof record.sourceID === "string" ? record.sourceID : undefined;
         out.push({ sourceID, ref: sourceID ?? node });
         return;
      }
      if (record.structRef !== undefined) {
         out.push({
            sourceID: structRefSourceId(record.structRef),
            ref: record.structRef,
         });
      }
      for (const [key, value] of Object.entries(record)) {
         if (key === "structRef") continue;
         walk(value, depth + 1);
      }
   };
   walk(pipeline, 0);
   return tooDeep ? undefined : out;
}

// ---------------------------------------------------------------------------
// Reachability: which declarations a set of sources reads
// ---------------------------------------------------------------------------

/**
 * Every `sourceID` a set of published sources (or named queries) reads,
 * following the declared edges: each node's identity and rename, the source it
 * extends, its declaration, a query-shaped source's base and pipeline, a
 * composite's resolved member, and every join (named or embedded) on a source
 * or in a view's pipeline. A pipeline too deep to read is scanned whole, so the
 * answer never says "not reached" for something it could not read.
 *
 * Read by the unreachable verdict and warning: a wrong `unreachable` is a false
 * alarm, so reachability errs toward reached.
 */
export function reachableSourceIDs(
   roots: readonly unknown[],
   modelDef?: unknown,
): Set<string> {
   const ids = new Set<string>();
   const seen = new Set<unknown>();
   const add = (value: unknown): void => {
      if (typeof value === "string" && value.length > 0) ids.add(value);
   };
   const scanWhole = (node: unknown, depth: number): void => {
      if (
         depth > MAX_PIPELINE_DEPTH ||
         node === null ||
         typeof node !== "object"
      )
         return;
      if (Array.isArray(node)) {
         for (const item of node) scanWhole(item, depth + 1);
         return;
      }
      const record = node as Record<string, unknown>;
      for (const key of ["sourceID", "referenceID", "extends"])
         add(record[key]);
      for (const value of Object.values(record)) scanWhole(value, depth + 1);
   };
   const visitPipeline = (pipeline: unknown): void => {
      if (pipeline === undefined) return;
      const refs = pipelineReferences(pipeline);
      if (refs === undefined) {
         scanWhole(pipeline, 0);
         return;
      }
      for (const { sourceID, ref } of refs) {
         add(sourceID);
         if (ref !== null && typeof ref === "object") visit(ref);
      }
   };
   const visit = (node: unknown): void => {
      if (node === null || typeof node !== "object" || seen.has(node)) return;
      seen.add(node);
      const def = node as MappedSourceDef & {
         pipeline?: unknown;
         structRef?: unknown;
      };
      add(def.sourceID);
      add(def.referenceID);
      add(def.extends);
      // A named query (or a turtle) carries its own structRef and pipeline.
      if (def.structRef !== undefined) {
         add(structRefSourceId(def.structRef));
         if (typeof def.structRef === "object") visit(def.structRef);
      }
      visitPipeline(def.pipeline);
      if (def.query) {
         add(structRefSourceId(def.query.structRef));
         if (def.query.structRef && typeof def.query.structRef === "object") {
            visit(def.query.structRef);
         }
         visit(def.query.compositeResolvedSourceDef);
         visitPipeline(def.query.pipeline);
      }
      // A composite's members: each one is read whenever the composite is.
      for (const member of def.sources ?? []) {
         add(structRefSourceId(member));
         visit(
            typeof member === "string"
               ? byId(modelDef as IrModel | undefined, member)
               : member,
         );
      }
      if (modelDef !== undefined && isSourceDef(node as SourceDef)) {
         const struct = node as SourceDef;
         const model = modelDef as ModelDef;
         const declared = resolveDeclaredSource(struct, model);
         if (declared.kind === "resolved") visit(declared.source);
         visit(resolveExtendedSource(struct, model));
         // An extension of a table-backed source records no `extends`.
         visit(inferredExtendedSource(struct, model));
         visit(resolveQuerySourceBase(struct, model));
         visit(resolveCompositeResolvedBase(struct));
      }
      for (const field of def.fields ?? []) {
         const f = field as {
            join?: unknown;
            sourceID?: unknown;
            pipeline?: unknown;
         };
         if (typeof f?.join === "string") {
            add(f.sourceID);
            visit(field);
         } else if (f?.pipeline !== undefined) {
            visitPipeline(f.pipeline);
         }
      }
   };
   for (const root of roots) visit(root);
   return ids;
}

// ---------------------------------------------------------------------------
// The build-side reach: persisted sources and raw leaves
// ---------------------------------------------------------------------------

/** A model's sources by name, and the name of each sourceID, as a build walk reads them. */
interface ReachContext {
   /** The model's `contents`, by source name. */
   contents: Record<string, MappedSourceDef>;
   /** sourceID -> author source name. */
   sourceNameById: Map<string, string>;
}

/** What {@link reachPersisted} found on the paths out of a source. */
interface ReachedSources {
   /**
    * The persist sources the walk stopped at. Names are neither tables (several
    * share one) nor package-unique, so the caller resolves each to its content
    * address by `sourceID`.
    */
   persisted: { name: string; sourceID: string }[];
   /**
    * Some path ends at a source with no stored table behind it (a table or SQL
    * source, or a reference with no in-model identity).
    */
   raw: boolean;
   /** The sources those paths ended at (`regions`), for the reason text. */
   rawLeaves: string[];
   /** The non-persisted sources that reach them (`daily_regional`). */
   rawVia: string[];
   /**
    * Every named source the walk passed through or stopped at, root excluded:
    * exactly the sources a build over the stored parents needs declared.
    */
   visited: string[];
}

function fieldKey(field: unknown): string {
   const f = field as { as?: unknown; name?: unknown };
   if (typeof f?.as === "string") return f.as;
   return typeof f?.name === "string" ? f.name : "";
}

/**
 * What a source reaches, walking from `name` through the sources it is built
 * from and stopping at each persist source, as `isPersisted` (answered from the
 * build plan) says.
 *
 * The plan, not the annotation, decides: `#@ persist` is inherited, so an
 * `extend` or rename of a persisted source shares its table, and nothing on a
 * definition says which name "declared" it. The walk records names; the caller
 * resolves each to its address group by `sourceID`.
 *
 * An extension's own joins are NOT in its table: `daily_regional is daily
 * extend { join_one: r is regions … }` computes the join at read time, so the
 * walk still follows the joins a persisted extension adds, and nothing else.
 *
 * An embedded definition (inline extension, join written in place) is walked
 * as itself; only a reference with no identity, or to a name the model lacks,
 * is raw.
 *
 * A chained build's refusal turns on this: a stored table not bound is an
 * upstream `strictUpstreams` refuses; a raw path can only be recomputed from
 * the warehouse; a source reaching neither that still cannot be carried is a
 * carrying limit, which strict must not treat as licence to recompute.
 */
export function reachPersisted(
   ctx: ReachContext,
   name: string,
   isPersisted: (sourceName: string, sourceID: string) => boolean,
): ReachedSources {
   const persisted = new Map<string, { name: string; sourceID: string }>();
   const rawLeaves = new Set<string>();
   const rawVia = new Set<string>();
   const visited = new Set<string>();
   const seen = new Set<unknown>();
   const defName = (def: MappedSourceDef): string | undefined => {
      const id = def.sourceID;
      return typeof id === "string" ? ctx.sourceNameById.get(id) : undefined;
   };
   const reachRaw = (leaf: string, via: string): void => {
      rawLeaves.add(leaf);
      rawVia.add(via);
   };
   // A `sourceID` is `name@<model URL>`; only the name may appear in a reason,
   // since the URL is the server's file path.
   const nameOf = (sourceID: string): string => sourceID.split("@")[0];
   // A reference is a sourceID, an embedded definition, or nothing usable.
   const follow = (ref: unknown, via: string): void => {
      if (typeof ref === "string") {
         const next = ctx.sourceNameById.get(ref);
         if (next !== undefined) {
            visit(ctx.contents[next], next, false, via);
            return;
         }
         // Not in this model's view (e.g. via an import) but known to the plan
         // as a persist source: a stop, never raw.
         const refName = nameOf(ref);
         if (isPersisted(refName, ref)) {
            visited.add(refName);
            persisted.set(ref, { name: refName, sourceID: ref });
            return;
         }
         reachRaw(refName, via);
         return;
      }
      if (ref !== null && typeof ref === "object") {
         const def = ref as MappedSourceDef;
         visit(def, defName(def), false, via);
         return;
      }
      reachRaw("(unnamed)", via);
   };
   const followJoins = (
      fields: unknown[] | undefined,
      sourceName: string,
   ): void => {
      for (const field of fields ?? []) {
         const f = field as { join?: unknown; sourceID?: unknown };
         if (typeof f.join !== "string") continue;
         follow(
            typeof f.sourceID === "string" ? f.sourceID : field,
            sourceName,
         );
      }
   };
   // The joins an extension adds: its join fields minus the ones its base
   // already carries, matched by the name a shape refers to them by.
   const ownJoins = (def: MappedSourceDef): unknown[] => {
      const baseName =
         typeof def.extends === "string"
            ? ctx.sourceNameById.get(def.extends)
            : undefined;
      const baseFields = new Set(
         (baseName ? (ctx.contents[baseName]?.fields ?? []) : []).map(fieldKey),
      );
      return (def.fields ?? []).filter((f) => !baseFields.has(fieldKey(f)));
   };
   // `sourceName` is undefined for an embedded definition. `via` is the source
   // that referenced this one, which the reason names alongside the leaf.
   const visit = (
      def: MappedSourceDef | undefined,
      sourceName: string | undefined,
      root: boolean,
      via: string,
   ): void => {
      const label = sourceName ?? via;
      if (!def) {
         reachRaw(label, via);
         return;
      }
      if (seen.has(def)) return;
      seen.add(def);
      if (!root && sourceName !== undefined) visited.add(sourceName);
      // The root is the source being built; only its descendants stop the walk.
      const sourceID = def.sourceID;
      if (
         !root &&
         sourceName !== undefined &&
         typeof sourceID === "string" &&
         isPersisted(sourceName, sourceID)
      ) {
         persisted.set(sourceID, { name: sourceName, sourceID });
         // Its table is its base's; only what it adds is computed over it.
         if (typeof def.extends === "string") {
            followJoins(ownJoins(def), sourceName);
         }
         return;
      }
      const base =
         typeof def.extends === "string"
            ? def.extends
            : def.type === "query_source"
              ? def.query?.structRef
              : undefined;
      if (base === undefined) {
         // A table or a SQL source: nothing stored stands behind it.
         reachRaw(label, via);
         return;
      }
      follow(base, label);
      // An extension's `query` is its base's, copied; the base is walked above.
      if (typeof def.extends !== "string") {
         const refs = pipelineReferences(def.query?.pipeline);
         // A pipeline too deep to walk may reach anything, so it counts as
         // reaching a leaf with no identity: raw, never "nothing".
         for (const { ref } of refs ?? [{ ref: undefined }]) {
            follow(ref, label);
         }
      }
      followJoins(def.fields, label);
   };
   visit(ctx.contents[name], name, true, name);
   return {
      persisted: [...persisted.values()],
      raw: rawLeaves.size > 0,
      rawLeaves: [...rawLeaves],
      rawVia: [...rawVia],
      visited: [...visited],
   };
}
