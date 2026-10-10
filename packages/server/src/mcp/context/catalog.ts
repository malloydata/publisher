// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The catalog the find, connect, judge engine searches: one package's
 * queryable surface as nodes (sources, fields, views, joins, named queries) and
 * the edges between them (contains, joins, extends, depends on, uses).
 *
 * It is built from what the shipped get_context already collects
 * (`PackageIndex.directEntities`, the join topology and the per-source
 * context), so hidden fields, deny-all sources and collapsed aliases are
 * handled exactly once, by collectEntities. The compiled model adds the three
 * things that list does not carry: which file defines a source (so a source
 * imported into five files is one source, not five), which source a source
 * extends, and which fields an expression reads.
 *
 * A source's identity is its name plus the file that DEFINES it, read from the
 * compiled `sourceID`. The shipped index keys by (model path, source), so one
 * source imported by three files is three cards and its fields compete with
 * themselves; here it is one source with three model paths.
 */

import type { ModelDef } from "@malloydata/malloy";
import type { Relationship } from "@malloydata/malloy-interfaces";
import {
   comparePaths,
   entityId,
   modelPathOfUrl,
   sourceContextKey,
   type Entity,
   type JoinTopology,
   type PackageIndex,
} from "../tools/get_context_tool";

export type NodeKind = Entity["kind"];

/** One searchable thing in the catalog. */
export interface CatalogNode {
   /** Unique in the catalog: `kind:sourceKey:name`. */
   key: string;
   /** The wire id the response uses: `kind:source:name`. */
   id: string;
   kind: NodeKind;
   name: string;
   /** The owning source's name (a source node names itself). */
   source: string;
   /** The owning source's catalog key. */
   sourceKey: string;
   /** Every model file the node resolves in, codepoint order. */
   modelPaths: string[];
   /** `#(doc)` text only; safe to send to a model or an embedder. */
   doc: string;
   /** The response's description (may fall back to raw annotation lines). */
   displayDoc: string;
   dataType?: string;
   /** The authored expression, when the field has one. */
   code?: string;
   /** For a field of an inline-table join: the join path inside its source. */
   joinPath?: string;
   relationship?: Relationship;
   aliases?: string[];
   /** Keys of the nodes this node's expression reads. */
   dependsOn: string[];
   /** For a view: keys of the fields its output names. */
   uses: string[];
   /** A dimension tagged `#(index)`: its values may be indexed. */
   indexed: boolean;
   /**
    * With `collapseCopies`: the node this one is an unchanged copy of (a field
    * an extension inherits keeps its parent's compiled definition). A copy is
    * never searched; it is the same field under another source's name.
    */
   copyOf?: string;
   /** With `collapseCopies`: the copies of this node in extensions, by key. */
   copies?: string[];
   /** One shipped entity per model path, in `modelPaths` order. */
   entities: Entity[];
}

/** One step a join takes from a source. */
export interface JoinHop {
   /** Catalog key of the source reached. */
   to: string;
   /** Join aliases from the root, root excluded. */
   path: string[];
   /** Widest relationship on the path. */
   fanout: Relationship;
}

export interface CatalogSource {
   key: string;
   name: string;
   modelPaths: string[];
   /** The model file that defines it. */
   definedIn: string;
   node: CatalogNode;
   /** The source it extends, when the compiled model says so and it is in the catalog. */
   extendsKey?: string;
   /** An access gate (`#(authorize)`, `#(access_filter)`) or a secure given is in force. */
   gated: boolean;
   /** Keys of every node the source owns, sources excluded, in walk order. */
   fields: string[];
}

export type EdgeKind = "contains" | "joins" | "extends" | "depends" | "uses";

export interface CatalogEdge {
   kind: EdgeKind;
   from: string;
   to: string;
   /** On a `joins` edge: the join aliases from `from`. */
   path?: string[];
}

/** What the build did, for the load log and the index-status endpoint. */
export interface CatalogReport {
   sources: number;
   nodes: Record<string, number>;
   edges: Record<EdgeKind, number>;
   /** Nodes that resolve in more than one model file (one node, several paths). */
   multiFileNodes: number;
   indexedDimensions: number;
   gatedSources: number;
   /** Field references an expression makes that resolve to no catalog node. */
   unresolvedReferences: number;
   /** With `collapseCopies`: nodes folded into the field they copy. */
   copiesFolded: number;
}

export interface Catalog {
   nodes: Map<string, CatalogNode>;
   sources: Map<string, CatalogSource>;
   edges: CatalogEdge[];
   /** Joins out of each source, by source key; already deduplicated by path. */
   joins: Map<string, JoinHop[]>;
   /** Source key by sourceContextKey(modelPath, name), for mapping shipped entities in. */
   sourceKeyByContext: Map<string, string>;
   report: CatalogReport;
}

/** The part of a compiled model the catalog reads. */
export interface CatalogModel {
   modelPath: string;
   modelDef?: ModelDef;
   /** The model's SourceInfo list, for `#(index)` tags and view outputs. */
   sourceInfos: readonly unknown[];
}

export interface CatalogInput {
   /** The shipped index's direct entities: no joined copy assembly would rebuild. */
   entities: readonly Entity[];
   topology: JoinTopology;
   /** Per (model path, source): whether a gate is in force. */
   gatedByContext: ReadonlyMap<string, boolean>;
   models: readonly CatalogModel[];
   /**
    * PROVISIONAL, a diagnostic of the connect rule (`retrieval.fcj.collapseCopies`).
    * Fold a field an extension inherits unchanged into the field where it is
    * defined, so an extension competes only with the fields it adds.
    */
   collapseCopies?: boolean;
}

/** `name` or `as`, whichever the compiled entry is known by. */
function activeName(v: { name?: string; as?: string }): string | undefined {
   return v.as ?? v.name;
}

interface CompiledLocation {
   url?: string;
   range?: {
      start: { line: number; character: number };
      end: { line: number; character: number };
   };
}

interface CompiledSource {
   name?: string;
   as?: string;
   sourceID?: string;
   extends?: string;
   location?: CompiledLocation;
   fields?: Array<{
      name?: string;
      as?: string;
      e?: unknown;
      location?: CompiledLocation;
   }>;
}

/** A location as one comparable string, or undefined when the compiler gave none. */
function locationKey(loc: CompiledLocation | undefined): string | undefined {
   if (!loc?.url || !loc.range) return undefined;
   const { start, end } = loc.range;
   return `${loc.url}#${start.line}:${start.character}-${end.line}:${end.character}`;
}

/** Whether `inner` lies inside `outer`, in the same file. */
function contains(outer: CompiledLocation, inner: CompiledLocation): boolean {
   if (!outer.url || outer.url !== inner.url || !outer.range || !inner.range) {
      return false;
   }
   const before = (
      a: { line: number; character: number },
      b: { line: number; character: number },
   ) => a.line < b.line || (a.line === b.line && a.character <= b.character);
   return (
      before(outer.range.start, inner.range.start) &&
      before(inner.range.end, outer.range.end)
   );
}

function compiledSource(
   modelDef: ModelDef | undefined,
   name: string,
): CompiledSource | undefined {
   const contents = (modelDef as { contents?: Record<string, unknown> })
      ?.contents;
   if (!contents) return undefined;
   return Object.values(contents).find(
      (c) => activeName(c as CompiledSource) === name,
   ) as CompiledSource | undefined;
}

/** `name@url` split into its parts, as Malloy writes `sourceID`. */
function splitSourceId(
   id: string | undefined,
): { source: string; url: string } | undefined {
   if (typeof id !== "string") return undefined;
   const match = /^(.+)@([a-z][a-z0-9+.-]*:\/\/.*)$/i.exec(id);
   return match ? { source: match[1], url: match[2] } : undefined;
}

/** The catalog key of a source: its name and the file that defines it. */
export function sourceKeyOf(name: string, definedIn: string): string {
   return `${name}@${definedIn}`;
}

/** The catalog key of a node. */
export function nodeKeyOf(kind: string, sourceKey: string, name: string) {
   return `${kind}:${sourceKey}:${name}`;
}

/** Every `{node: "field", path}` an expression tree reads, in walk order. */
export function referencedPaths(e: unknown): string[][] {
   const out: string[][] = [];
   const walk = (v: unknown, depth: number) => {
      if (depth > 64 || v === null || typeof v !== "object") return;
      if (Array.isArray(v)) {
         for (const x of v) walk(x, depth + 1);
         return;
      }
      const o = v as Record<string, unknown>;
      if (o.node === "field" && Array.isArray(o.path)) {
         out.push(o.path.map(String));
      }
      for (const [k, x] of Object.entries(o)) {
         // `at` is a location, never an expression.
         if (k !== "at" && k !== "path") walk(x, depth + 1);
      }
   };
   walk(e, 0);
   return out;
}

/** Whether a field's annotations carry an `#(index)` tag. */
function hasIndexTag(field: {
   annotations?: Array<string | { value: string }>;
}): boolean {
   return (field.annotations ?? []).some((a) =>
      /^\s*#\(index\b/.test(typeof a === "string" ? a : a.value),
   );
}

interface InfoField {
   kind?: string;
   name: string;
   annotations?: Array<string | { value: string }>;
   schema?: { fields?: InfoField[] };
}

/**
 * Build the catalog. Pure over its input: no I/O, so every rule is testable
 * against a compiled fixture.
 */
export function buildCatalog(input: CatalogInput): Catalog {
   const modelPaths = input.models.map((m) => m.modelPath);
   const modelByPath = new Map(input.models.map((m) => [m.modelPath, m]));
   const sourceKeyByContext = new Map<string, string>();
   const sources = new Map<string, CatalogSource>();
   const nodes = new Map<string, CatalogNode>();
   const edges: CatalogEdge[] = [];

   // 1. Sources, keyed by where they are defined.
   const definitionOf = (modelPath: string, name: string) => {
      const compiled = compiledSource(
         modelByPath.get(modelPath)?.modelDef,
         name,
      );
      const id = splitSourceId(compiled?.sourceID);
      const definedIn =
         (id && id.source === name
            ? modelPathOfUrl(id.url, modelPaths)
            : undefined) ?? modelPath;
      const parent = splitSourceId(compiled?.extends);
      const parentIn = parent
         ? modelPathOfUrl(parent.url, modelPaths)
         : undefined;
      return {
         definedIn,
         extendsKey:
            parent && parentIn !== undefined
               ? sourceKeyOf(parent.source, parentIn)
               : undefined,
      };
   };
   const extendsOf = new Map<string, string>();
   for (const e of input.entities) {
      if (e.kind !== "source") continue;
      const { definedIn, extendsKey } = definitionOf(e.modelPath, e.name);
      const key = sourceKeyOf(e.name, definedIn);
      sourceKeyByContext.set(sourceContextKey(e.modelPath, e.name), key);
      const gated =
         input.gatedByContext.get(sourceContextKey(e.modelPath, e.name)) ??
         false;
      const existing = sources.get(key);
      if (existing) {
         existing.modelPaths.push(e.modelPath);
         existing.node.modelPaths.push(e.modelPath);
         existing.node.entities.push(e);
         existing.gated ||= gated;
         continue;
      }
      if (extendsKey) extendsOf.set(key, extendsKey);
      const node: CatalogNode = {
         key: nodeKeyOf("source", key, e.name),
         id: entityId("source", e.name, e.name),
         kind: "source",
         name: e.name,
         source: e.name,
         sourceKey: key,
         modelPaths: [e.modelPath],
         doc: e.embedDoc,
         displayDoc: e.doc,
         dependsOn: [],
         uses: [],
         indexed: false,
         entities: [e],
      };
      sources.set(key, {
         key,
         name: e.name,
         modelPaths: [e.modelPath],
         definedIn,
         node,
         gated,
         fields: [],
      });
      nodes.set(node.key, node);
   }
   for (const [child, parent] of extendsOf) {
      const source = sources.get(child);
      if (source && sources.has(parent) && parent !== child) {
         source.extendsKey = parent;
         edges.push({ kind: "extends", from: child, to: parent });
      }
   }

   // 2. Fields, views, joins and queries, one node per (kind, source, name).
   for (const e of input.entities) {
      if (e.kind === "source" || !e.source) continue;
      const sourceKey = sourceKeyByContext.get(
         sourceContextKey(e.modelPath, e.source),
      );
      if (!sourceKey) continue;
      const key = nodeKeyOf(e.kind, sourceKey, e.name);
      const existing = nodes.get(key);
      if (existing) {
         if (!existing.modelPaths.includes(e.modelPath)) {
            existing.modelPaths.push(e.modelPath);
            existing.entities.push(e);
         }
         continue;
      }
      const node: CatalogNode = {
         key,
         id: entityId(e.kind, e.source, e.name),
         kind: e.kind,
         name: e.name,
         source: e.source,
         sourceKey,
         modelPaths: [e.modelPath],
         doc: e.embedDoc,
         displayDoc: e.doc,
         ...(e.dataType ? { dataType: e.dataType } : {}),
         ...(e.code ? { code: e.code } : {}),
         ...(e.joinPath ? { joinPath: e.joinPath } : {}),
         ...(e.relationship ? { relationship: e.relationship } : {}),
         ...(e.aliases ? { aliases: e.aliases } : {}),
         dependsOn: [],
         uses: [],
         indexed: false,
         entities: [e],
      };
      nodes.set(key, node);
      sources.get(sourceKey)?.fields.push(key);
      edges.push({ kind: "contains", from: sourceKey, to: key });
   }
   for (const node of nodes.values()) {
      node.modelPaths.sort(comparePaths);
      node.entities.sort((a, b) => comparePaths(a.modelPath, b.modelPath));
   }
   for (const source of sources.values()) source.modelPaths.sort(comparePaths);

   // 3. Joins out of each source, from the shipped topology.
   const joins = new Map<string, JoinHop[]>();
   for (const [root, reaches] of input.topology) {
      const from = sourceKeyByContext.get(root);
      if (!from) continue;
      const hops = joins.get(from) ?? [];
      for (const reach of reaches) {
         const to = sourceKeyOf(reach.targetSource, reach.targetModelPath);
         if (!sources.has(to)) continue;
         const pathText = reach.path.join(".");
         if (hops.some((h) => h.path.join(".") === pathText)) continue;
         hops.push({ to, path: [...reach.path], fanout: reach.fanout });
      }
      if (hops.length === 0) continue;
      joins.set(from, hops);
   }
   for (const [from, hops] of joins) {
      for (const hop of hops) {
         if (hop.path.length === 1) {
            edges.push({ kind: "joins", from, to: hop.to, path: hop.path });
         }
      }
   }

   // 4. Tags, view outputs and expression references, from the compiled model.
   let unresolved = 0;
   const fieldNode = (sourceKey: string, name: string) =>
      nodes.get(nodeKeyOf("dimension", sourceKey, name)) ??
      nodes.get(nodeKeyOf("measure", sourceKey, name));
   const resolvePath = (
      sourceKey: string,
      path: string[],
   ): CatalogNode | undefined => {
      if (path.length === 1) return fieldNode(sourceKey, path[0]);
      // An inline-table join's field lives on the source under its dotted name.
      const dotted = fieldNode(sourceKey, path.join("."));
      if (dotted) return dotted;
      const via = path.slice(0, -1).join(".");
      const hop = joins.get(sourceKey)?.find((h) => h.path.join(".") === via);
      return hop ? fieldNode(hop.to, path[path.length - 1]) : undefined;
   };
   const seenSource = new Set<string>();
   // Where each node's definition is, and where each source's own definition
   // is, for collapseCopies.
   const definitionAt = new Map<string, CompiledLocation>();
   const sourceAt = new Map<string, CompiledLocation>();
   for (const model of input.models) {
      for (const raw of model.sourceInfos) {
         const info = raw as {
            name?: string;
            schema?: { fields?: InfoField[] };
         };
         if (!info.name) continue;
         const sourceKey = sourceKeyByContext.get(
            sourceContextKey(model.modelPath, info.name),
         );
         if (!sourceKey || seenSource.has(sourceKey)) continue;
         seenSource.add(sourceKey);
         for (const field of info.schema?.fields ?? []) {
            if (field.kind === "dimension" && hasIndexTag(field)) {
               const node = nodes.get(
                  nodeKeyOf("dimension", sourceKey, field.name),
               );
               if (node) node.indexed = true;
            }
            if (field.kind === "view") {
               const view = nodes.get(nodeKeyOf("view", sourceKey, field.name));
               if (!view) continue;
               for (const out of field.schema?.fields ?? []) {
                  const used = fieldNode(sourceKey, out.name);
                  if (used && !view.uses.includes(used.key)) {
                     view.uses.push(used.key);
                     edges.push({ kind: "uses", from: view.key, to: used.key });
                  }
               }
            }
         }
         const compiled = compiledSource(model.modelDef, info.name);
         if (compiled?.location) sourceAt.set(sourceKey, compiled.location);
         for (const f of compiled?.fields ?? []) {
            const name = activeName(f);
            if (!name) continue;
            const located =
               fieldNode(sourceKey, name) ??
               nodes.get(nodeKeyOf("view", sourceKey, name));
            if (located && f.location)
               definitionAt.set(located.key, f.location);
            if (!f.e) continue;
            const node = fieldNode(sourceKey, name);
            if (!node) continue;
            for (const path of referencedPaths(f.e)) {
               const target = resolvePath(sourceKey, path);
               if (!target) {
                  unresolved += 1;
                  continue;
               }
               if (
                  target.key === node.key ||
                  node.dependsOn.includes(target.key)
               ) {
                  continue;
               }
               node.dependsOn.push(target.key);
               edges.push({ kind: "depends", from: node.key, to: target.key });
            }
         }
      }
   }

   // 5. With collapseCopies, fold each unchanged inherited copy into the node
   // of the source whose own definition holds the field's definition.
   let copiesFolded = 0;
   if (input.collapseCopies) {
      const groups = new Map<string, CatalogNode[]>();
      for (const node of nodes.values()) {
         if (node.kind === "source" || node.joinPath) continue;
         const at = locationKey(definitionAt.get(node.key));
         if (!at) continue;
         const key = `${node.kind}\u0000${node.name}\u0000${at}`;
         const list = groups.get(key) ?? [];
         list.push(node);
         groups.set(key, list);
      }
      for (const list of groups.values()) {
         if (list.length < 2) continue;
         const at = definitionAt.get(list[0].key)!;
         const definer = list.find((n) => {
            const own = sourceAt.get(n.sourceKey);
            return own ? contains(own, at) : false;
         });
         if (!definer) continue;
         for (const n of list) {
            if (n === definer) continue;
            n.copyOf = definer.key;
            (definer.copies ??= []).push(n.key);
            copiesFolded += 1;
         }
      }
   }

   const nodeCounts: Record<string, number> = {};
   for (const n of nodes.values())
      nodeCounts[n.kind] = (nodeCounts[n.kind] ?? 0) + 1;
   const edgeCounts: Record<EdgeKind, number> = {
      contains: 0,
      joins: 0,
      extends: 0,
      depends: 0,
      uses: 0,
   };
   for (const e of edges) edgeCounts[e.kind] += 1;
   return {
      nodes,
      sources,
      edges,
      joins,
      sourceKeyByContext,
      report: {
         sources: sources.size,
         nodes: nodeCounts,
         edges: edgeCounts,
         multiFileNodes: [...nodes.values()].filter(
            (n) => n.modelPaths.length > 1,
         ).length,
         indexedDimensions: [...nodes.values()].filter((n) => n.indexed).length,
         gatedSources: [...sources.values()].filter((s) => s.gated).length,
         unresolvedReferences: unresolved,
         copiesFolded,
      },
   };
}

/**
 * The catalog input for a served package: the shipped index's entities and
 * topology plus each model's compiled IR and SourceInfo. Read defensively,
 * because a spec's package stand-in implements only the accessors it needs.
 */
export async function catalogInputOf(
   pkgIndex: PackageIndex,
): Promise<CatalogInput> {
   const pkg = pkgIndex.pkg as unknown as {
      listModels: () => Promise<Array<{ path?: string }>>;
      getModel: (path: string) =>
         | {
              getModelDef?: () => ModelDef | undefined;
              getSourceInfos?: () => unknown[] | undefined;
           }
         | undefined;
   };
   const listed = await pkg.listModels();
   const models: CatalogModel[] = [];
   for (const m of [...listed].sort((a, b) =>
      comparePaths(a.path ?? "", b.path ?? ""),
   )) {
      if (!m.path) continue;
      const model = pkg.getModel(m.path);
      if (!model) continue;
      models.push({
         modelPath: m.path,
         modelDef: model.getModelDef?.(),
         sourceInfos: model.getSourceInfos?.() ?? [],
      });
   }
   const gatedByContext = new Map<string, boolean>();
   for (const [key, ctx] of pkgIndex.sourceContext) {
      gatedByContext.set(
         key,
         Boolean(ctx.accessFilter?.length) ||
            Boolean(ctx.authorize?.length) ||
            (ctx.givens ?? []).some((g) =>
               (g.annotations ?? []).some((a) => /secure/i.test(a)),
            ),
      );
   }
   return {
      entities: pkgIndex.directEntities,
      topology: pkgIndex.topology,
      gatedByContext,
      models,
   };
}
