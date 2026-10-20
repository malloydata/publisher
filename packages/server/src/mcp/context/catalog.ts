// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The catalog the find, connect, judge engine searches: one package's
 * queryable surface as nodes (sources, fields, views, joins, named queries) and
 * the edges between them (contains, joins, extends, depends on, uses).
 *
 * It is built from what get_context's entity index already collects
 * (`PackageIndex.directEntities`, the join topology and the per-source
 * context), so hidden fields, deny-all sources and collapsed aliases are
 * handled exactly once, by collectEntities. The compiled model adds the three
 * things that list does not carry: which file defines a source (so a source
 * imported into five files is one source, not five), which source a source
 * extends, and which fields an expression reads.
 *
 * A source's identity is its name plus the file that DEFINES it, read from the
 * compiled `sourceID`. The entity index keys by (model path, source), so one
 * source imported by three files is three cards and its fields compete with
 * themselves; here it is one source with three model paths.
 */

import type { ModelDef } from "@malloydata/malloy";
import type { Relationship } from "@malloydata/malloy-interfaces";
import { compareText as comparePaths } from "../tools/retrieval_shared";
import {
   docOnlyText,
   docText,
   enterJoin,
   modelPathOfUrl,
   sourceContextKey,
} from "../tools/package_entities";
import { entityId } from "../tools/get_context_response";
import {
   type Entity,
   type JoinTopology,
   type PackageIndex,
} from "../tools/get_context_types";

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
   /**
    * For a field of a join (`joinPath`): the catalog key of the source the
    * join is made from, read from the compiled join's `sourceID` (or the
    * source it extends, for `join_one: x is s extend {...}`). Absent for a
    * raw table or SQL join, or a source outside the catalog.
    */
   joinedSourceKey?: string;
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
    * an extension inherits keeps its parent's compiled definition), on a
    * source with the same rows as this node's source (see sameRows). A copy
    * is never searched; it is the same field under another source's name.
    */
   copyOf?: string;
   /** With `collapseCopies`: the copies of this node in extensions, by key. */
   copies?: string[];
   /**
    * With `collapseCopies`: shared by the fields that are one inherited
    * definition left on several sources whose rows differ (near-identical
    * sources, `twinRule`). Absent on a field with no such twin.
    */
   twinKey?: string;
   /** One entity per model path, in `modelPaths` order. */
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
   /**
    * With `collapseCopies`: inherited fields kept on their own source, because
    * its rows differ from its parent's (a `where:`, another base, other
    * arguments, or a gate of its own) or its parent is not in the catalog
    * (hidden or unserved).
    */
   copiesKeptApart: number;
   /**
    * Sources left out because they are marked `#(agent-hidden)` (on the
    * source itself, or `##(agent-hidden)` on the file that defines it). Their
    * fields are reached only through a served source's joins.
    */
   hiddenSources: number;
   /** Join paths the walk recorded, over every source. */
   joinPaths: number;
   /** Joins in the longest recorded path. */
   deepestJoinPath: number;
   /**
    * Paths the walk did not extend because they already had JOIN_WALK_MAX_HOPS
    * joins (or a source already had JOIN_WALK_MAX_PATHS paths). 0 means every
    * path of the join graph is recorded.
    */
   joinPathsCapped: number;
   /**
    * With `joinedCopies: "routes"`: joined copies left out because the field
    * has a node on the source the join reaches (a route reaches it instead).
    */
   joinedCopiesDropped: number;
   /**
    * With `unservedCopies`: joined copies added for fields of unserved
    * sources (beyond what the entity index has), copies left out by the
    * per-field cap, and join paths the walk stopped at its guard.
    */
   unservedCopiesAdded: number;
   unservedCopiesCapped: number;
   unservedWalkCapped: number;
}

export interface Catalog {
   nodes: Map<string, CatalogNode>;
   sources: Map<string, CatalogSource>;
   edges: CatalogEdge[];
   /**
    * Joins out of each source, by source key: every path the join graph has
    * from it, at any depth up to JOIN_WALK_MAX_HOPS (see walkJoins). One
    * entry per path.
    */
   joins: Map<string, JoinHop[]>;
   /**
    * Deepest route routesOf lists: ROUTE_MAX_DEPTH with
    * `joinedCopies: "indexed"`, JOIN_WALK_MAX_HOPS with `"routes"`.
    */
   routeDepth: number;
   /** The `joinedCopies` mode the catalog was built in. */
   joinedCopies: "indexed" | "routes";
   /** Source key by sourceContextKey(modelPath, name), for mapping entities in. */
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
   /** The entity index's direct entities: no joined copy assembly would rebuild. */
   entities: readonly Entity[];
   topology: JoinTopology;
   /** Per (model path, source): whether a gate is in force. */
   gatedByContext: ReadonlyMap<string, boolean>;
   models: readonly CatalogModel[];
   /**
    * PROVISIONAL, a diagnostic of the connect rule (`retrieval.fcj.collapseCopies`).
    * Fold a field an extension inherits unchanged into the same field on the
    * source it extends, when both sources hold the same rows (sameRows), so
    * an extension that only adds fields competes only with the fields it
    * adds. An extension with rows of its own (a `where:`, another base) is a
    * different source, and every field it inherits stays its own.
    */
   collapseCopies?: boolean;
   /**
    * PROVISIONAL (`retrieval.fcj.joinedCopies`), `indexed` when absent. With
    * `routes`, a joined copy of a field that has a node on the source the
    * join reaches is left out, a join to a source extended in the join
    * (`b is s extend {...}`) becomes a join hop to `s`, and routes are listed
    * at any depth.
    */
   joinedCopies?: "indexed" | "routes";
   /**
    * PROVISIONAL (`retrieval.fcj.unservedCopies`, 3 by default there), 0
    * when absent from this input. Above 0, a
    * field whose own source is not served (an unpublished source, or a raw
    * table or SQL join) gets a joined copy on each served source that reaches
    * it, at any depth up to JOIN_WALK_MAX_HOPS joins (JOIN_WALK_MAX_PATHS
    * join paths walked per source), at most this many copies per field,
    * shortest path first. The copies the entity index already has (two
    * joins deep) are kept and count toward that cap.
    */
   unservedCopies?: number;
}

/** Most joins in one path the join walk records. */
export const JOIN_WALK_MAX_HOPS = 8;
/** Most paths the join walk records from one source. */
export const JOIN_WALK_MAX_PATHS = 1_000;
/**
 * Deepest route listed with `joinedCopies: "indexed"`: the depth the entity
 * index gives joined copies.
 */
export const ROUTE_MAX_DEPTH = 2;

function byText(a: string, b: string): number {
   return a < b ? -1 : a > b ? 1 : 0;
}

/**
 * Every join path out of every source, walked over the join graph to any
 * depth: a path is one of the source's own joins, then one of the joins of
 * the source that join reaches, and so on. `recorded` holds what the compiled
 * model records per source: its own joins (paths of one join), plus nested
 * paths, which are kept as they are when the one-join walk cannot produce
 * them (their first join has no source to step through).
 *
 * A path never passes through a source twice. A join back to a source already
 * on the path (a source joined to itself, or a cycle) is recorded as the last
 * join of that path and not followed, so `manager` is a path and
 * `manager.manager` is not. A path stops at JOIN_WALK_MAX_HOPS joins, and a
 * source at JOIN_WALK_MAX_PATHS paths; `capped` counts the stops. Pure.
 */
export function walkJoins(recorded: ReadonlyMap<string, readonly JoinHop[]>): {
   joins: Map<string, JoinHop[]>;
   capped: number;
} {
   const edges = new Map<string, JoinHop[]>();
   for (const [from, hops] of recorded) {
      edges.set(
         from,
         hops
            .filter((h) => h.path.length === 1)
            .sort((a, b) => byText(a.path[0], b.path[0])),
      );
   }
   let capped = 0;
   const joins = new Map<string, JoinHop[]>();
   for (const root of recorded.keys()) {
      const out: JoinHop[] = [];
      const byPath = new Map<string, string>();
      let full = false;
      const visit = (
         at: string,
         path: readonly string[],
         onPath: ReadonlySet<string>,
         fanout: Relationship | undefined,
      ) => {
         for (const edge of edges.get(at) ?? []) {
            if (out.length >= JOIN_WALK_MAX_PATHS) {
               if (!full) capped += 1;
               full = true;
               return;
            }
            const next = [...path, edge.path[0]];
            // A `one` join keeps the fan-out so far; any other replaces it.
            const f: Relationship =
               fanout !== undefined && edge.fanout === "one"
                  ? fanout
                  : edge.fanout;
            out.push({ to: edge.to, path: next, fanout: f });
            byPath.set(next.join("."), edge.to);
            if (onPath.has(edge.to)) continue;
            if (next.length >= JOIN_WALK_MAX_HOPS) {
               if ((edges.get(edge.to) ?? []).length > 0) capped += 1;
               continue;
            }
            visit(edge.to, next, new Set([...onPath, edge.to]), f);
         }
      };
      visit(root, [], new Set([root]), undefined);
      for (const hop of recorded.get(root) ?? []) {
         const text = hop.path.join(".");
         if (byPath.has(text)) continue;
         if (
            hop.path.length > JOIN_WALK_MAX_HOPS ||
            out.length >= JOIN_WALK_MAX_PATHS
         ) {
            capped += 1;
            continue;
         }
         // The same rule: no source twice before the path's last join.
         const seen = new Set([root]);
         let repeats = false;
         for (let i = 1; i < hop.path.length && !repeats; i++) {
            const prefix = hop.path.slice(0, i).join(".");
            const via =
               byPath.get(prefix) ??
               recorded.get(root)?.find((h) => h.path.join(".") === prefix)?.to;
            if (via === undefined) continue;
            repeats = seen.has(via);
            seen.add(via);
         }
         if (repeats) continue;
         out.push({ to: hop.to, path: [...hop.path], fanout: hop.fanout });
         byPath.set(text, hop.to);
      }
      if (out.length > 0) joins.set(root, out);
   }
   return { joins, capped };
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

interface CompiledNotes {
   blockNotes?: Array<{ text?: string; at?: CompiledLocation }>;
   inherits?: CompiledNotes;
}

interface CompiledField {
   name?: string;
   as?: string;
   e?: unknown;
   location?: CompiledLocation;
   /** Set on a join entry. */
   join?: string;
   sourceID?: string;
   extends?: string;
   fields?: CompiledField[];
}

interface CompiledSource {
   name?: string;
   as?: string;
   sourceID?: string;
   extends?: string;
   /** Set when the source is an unmodified reference (`source: a is b`). */
   referenceID?: string;
   location?: CompiledLocation;
   annotations?: CompiledNotes;
   fields?: CompiledField[];
   /** What the rows come from: `table`, `sql_select`, `query_source`, ... */
   type?: string;
   tablePath?: string;
   selectStr?: string;
   query?: unknown;
   connection?: string;
   /** Every `where:` in force, the parent's first (an extension adds to it). */
   filterList?: Array<{ code?: string }>;
   arguments?: unknown;
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

/** Every block note of an annotation and of everything it inherits. */
function allNotes(
   notes: CompiledNotes | undefined,
   depth = 0,
): Array<{ text?: string; at?: CompiledLocation }> {
   if (!notes || depth > 32) return [];
   return [...(notes.blockNotes ?? []), ...allNotes(notes.inherits, depth + 1)];
}

function noteKey(note: { text?: string; at?: CompiledLocation }): string {
   return `${locationKey(note.at) ?? ""}\u0000${note.text ?? ""}`;
}

/**
 * The `#(doc)` lines an extension writes itself, or undefined when the source
 * extends nothing or its parent cannot be read (the doc then stands).
 *
 * The compiler keeps an extension's own notes apart from its parent's
 * (`blockNotes` and `inherits.blockNotes`) only when it has notes of its own.
 * An extension with none carries its parent's annotation unchanged, so its
 * `blockNotes` are the parent's and read as its own doc. A note is the
 * extension's own when the parent, or anything the parent inherits, does not
 * hold the same note at the same place.
 */
function ownNotesOf(
   modelDef: ModelDef | undefined,
   name: string,
): string[] | undefined {
   const compiled = compiledSource(modelDef, name);
   if (!compiled?.extends) return undefined;
   const contents = (modelDef as { contents?: Record<string, unknown> })
      ?.contents;
   const parent = Object.values(contents ?? {}).find(
      (c) => (c as CompiledSource).sourceID === compiled.extends,
   ) as CompiledSource | undefined;
   if (!parent) return undefined;
   const inherited = new Set(allNotes(parent.annotations).map(noteKey));
   return (compiled.annotations?.blockNotes ?? [])
      .filter((n) => !inherited.has(noteKey(n)))
      .map((n) => n.text ?? "")
      .filter(Boolean);
}

/** A compiled source anywhere in the package, by its `sourceID` (`name@url`). */
function compiledById(
   models: readonly CatalogModel[],
   id: string | undefined,
): CompiledSource | undefined {
   if (!id) return undefined;
   for (const model of models) {
      const contents = (
         model.modelDef as { contents?: Record<string, unknown> }
      )?.contents;
      for (const c of Object.values(contents ?? {})) {
         if ((c as CompiledSource).sourceID === id) return c as CompiledSource;
      }
   }
   return undefined;
}

/** The source a source was made from: the one it extends or plainly names. */
function parentIdOf(compiled: CompiledSource | undefined): string | undefined {
   return compiled?.extends ?? compiled?.referenceID;
}

/** Start lines of every compiled source definition, by file URL. */
const sourceStartsCache = new WeakMap<
   readonly CatalogModel[],
   Map<string, number[]>
>();
function sourceStarts(models: readonly CatalogModel[]): Map<string, number[]> {
   const cached = sourceStartsCache.get(models);
   if (cached) return cached;
   const out = new Map<string, Set<number>>();
   for (const model of models) {
      const contents = (
         model.modelDef as { contents?: Record<string, unknown> }
      )?.contents;
      for (const c of Object.values(contents ?? {})) {
         const loc = (c as CompiledSource).location;
         if (!loc?.url || !loc.range) continue;
         const set = out.get(loc.url) ?? new Set<number>();
         set.add(loc.range.start.line);
         out.set(loc.url, set);
      }
   }
   const sorted = new Map(
      [...out].map(([u, set]) => [u, [...set].sort((a, b) => a - b)]),
   );
   sourceStartsCache.set(models, sorted);
   return sorted;
}

/**
 * The notes a source writes itself. The compiler hands an extension (or a
 * plain reference) with no notes of its own its parent's notes as its own
 * `blockNotes`. When the parent is known (`extends`, `referenceID`), a note
 * is the source's own when the parent, or anything the parent inherits,
 * does not hold the same note at the same place. A table source's
 * extension does not record its parent; then a note is the source's own
 * when it sits in the source's file above the source's definition, with no
 * other source definition in between.
 */
function ownNoteTexts(
   models: readonly CatalogModel[],
   compiled: CompiledSource,
): string[] {
   const notes = compiled.annotations?.blockNotes ?? [];
   const parent = compiledById(models, parentIdOf(compiled));
   let own: typeof notes;
   if (parent) {
      const inherited = new Set(allNotes(parent.annotations).map(noteKey));
      own = notes.filter((n) => !inherited.has(noteKey(n)));
   } else {
      const loc = compiled.location;
      const starts = loc?.url ? (sourceStarts(models).get(loc.url) ?? []) : [];
      own = notes.filter((n) => {
         if (!loc?.range || !n.at?.range || n.at.url !== loc.url) {
            // Nothing to place it by: read as the source's own only when
            // the source has no location to compare against either.
            return !loc?.range;
         }
         const line = n.at.range.start.line;
         const start = loc.range.start.line;
         return (
            line <= start &&
            !starts.some((other) => other > line && other < start)
         );
      });
   }
   return own.map((n) => n.text ?? "").filter(Boolean);
}

/** `#(agent-hidden)` on a source; any bracket pair Malloy routes, as hosted reads it. */
const AGENT_HIDDEN_SOURCE = /^#\|?\s*[([{<]?\s*agent-hidden\b/;
/** `##(agent-hidden)` on a model file: hides the sources that file defines. */
const AGENT_HIDDEN_FILE = /^##\|?\s*[([{<]?\s*agent-hidden\b/;
/** An access gate a source writes itself: it changes which rows a caller sees. */
const OWN_GATE = /^#\|?\s*[([{<]?\s*(access_filter|authorize)\b/;

/**
 * Whether a source is marked `#(agent-hidden)`: on its own notes (a parent's
 * mark never carries through `extend`), or `##(agent-hidden)` on the file
 * that defines it (never on a file that only imports it). Deprecated, but
 * models still use it to keep internal bases out of retrieval, and
 * hosted get_context honours it the same way.
 */
function isAgentHidden(
   models: readonly CatalogModel[],
   compiled: CompiledSource | undefined,
): boolean {
   if (!compiled) return false;
   if (ownNoteTexts(models, compiled).some((t) => AGENT_HIDDEN_SOURCE.test(t)))
      return true;
   const url = splitSourceId(compiled.sourceID)?.url ?? compiled.location?.url;
   if (!url) return false;
   for (const model of models) {
      const file = (
         model.modelDef as {
            modelAnnotations?: Record<
               string,
               { ownNotes?: { notes?: Array<{ text?: string }> } }
            >;
         }
      )?.modelAnnotations?.[url];
      if (file) {
         return (file.ownNotes?.notes ?? []).some((n) =>
            AGENT_HIDDEN_FILE.test(n.text ?? ""),
         );
      }
   }
   return false;
}

/**
 * Whether a source holds exactly the rows of the source it was made from, so
 * a field it inherits unchanged is the same field with the same values.
 * True only when nothing that picks rows differs: the same kind of base
 * (table, SQL, query), the same table, SQL or query, the same `where:`
 * filters, the same arguments, and no access gate of its own. Adding fields,
 * renaming, hiding fields or adding joins keeps the rows (a Malloy join
 * never changes which rows a source's measures aggregate). Anything unread
 * or unclear answers false: two sources are kept apart when in doubt.
 */
export function sameRows(
   child: CompiledSource | undefined,
   parent: CompiledSource | undefined,
   childOwnNotes: readonly string[],
): boolean {
   if (!child || !parent || !child.type || child.type !== parent.type) {
      return false;
   }
   const base = (s: CompiledSource) => {
      switch (s.type) {
         case "table":
            return s.tablePath;
         case "sql_select":
            return s.selectStr;
         case "query_source":
            return JSON.stringify(s.query);
         default:
            return undefined;
      }
   };
   const childBase = base(child);
   if (childBase === undefined || childBase !== base(parent)) return false;
   if ((child.connection ?? "") !== (parent.connection ?? "")) return false;
   const filters = (s: CompiledSource) =>
      JSON.stringify((s.filterList ?? []).map((f) => f.code ?? null));
   if (filters(child) !== filters(parent)) return false;
   // No arguments is written as absent or as `{}`.
   const args = (s: CompiledSource) =>
      s.arguments &&
      typeof s.arguments === "object" &&
      Object.keys(s.arguments).length > 0
         ? JSON.stringify(s.arguments)
         : "";
   if (args(child) !== args(parent)) return false;
   return !childOwnNotes.some((t) => OWN_GATE.test(t));
}

/**
 * By join path (`a`, `a.b`): the id of the source each compiled join is made
 * from, `name@url`. A join on a named source carries it as `sourceID`; a join
 * on `s extend {...}` names `s` in `extends`; a raw table or SQL join has
 * neither.
 */
function joinSourceIds(
   fields: CompiledField[] | undefined,
   prefix: readonly string[] = [],
   out = new Map<string, string>(),
   /** Filled with each join path's relationship (`one`, `many`, `cross`). */
   kinds?: Map<string, string>,
): Map<string, string> {
   if (prefix.length >= 10) return out;
   for (const f of fields ?? []) {
      if (f.join === undefined) continue;
      const name = activeName(f);
      if (!name) continue;
      const path = [...prefix, name];
      const id = f.sourceID ?? f.extends;
      if (id) out.set(path.join("."), id);
      kinds?.set(path.join("."), f.join);
      joinSourceIds(f.fields, path, out, kinds);
   }
   return out;
}

/** The widest relationship on a compiled join path, as enterJoin computes it. */
function compiledFanout(
   kinds: ReadonlyMap<string, string>,
   path: readonly string[],
): Relationship {
   let fanout: Relationship = "one";
   for (let i = 1; i <= path.length; i++) {
      const kind = kinds.get(path.slice(0, i).join(".")) as
         | Relationship
         | undefined;
      if (i === 1) fanout = kind ?? "one";
      else if (kind && kind !== "one") fanout = kind;
   }
   return fanout;
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
   relationship?: Relationship;
   type?: { kind?: string };
}

/**
 * The joined copies `unservedCopies` adds: for each served source, every
 * dimension and measure its joins reach whose own source is not served, at
 * any depth up to JOIN_WALK_MAX_HOPS joins. A field's own source is the
 * source of the last join on its path that names one (`sourceID`, or the
 * source a `join_one: x is s extend {...}` extends); a raw table or SQL join
 * belongs to the source above it. A field of a served source is left out:
 * it is found once, on its own source, with routes.
 *
 * Each field gets at most `cap` copies over all served sources, shortest
 * path first (then source key, then path). The copies the catalog already
 * holds (the entity index's, two joins deep) are kept and count first. The
 * walk records at most JOIN_WALK_MAX_PATHS join paths per served source.
 */
function unservedCopies(args: {
   models: readonly CatalogModel[];
   modelPaths: readonly string[];
   sourceKeyByContext: ReadonlyMap<string, string>;
   sources: ReadonlyMap<string, CatalogSource>;
   hasNode: (kind: string, sourceKey: string, name: string) => boolean;
   cap: number;
}): {
   added: Array<{ entity: Entity; sourceKey: string }>;
   capped: number;
   walkCapped: number;
} {
   const { modelPaths, sources, hasNode, cap } = args;
   const keyOfId = (id: string | undefined) => {
      const parsed = splitSourceId(id);
      const at = parsed ? modelPathOfUrl(parsed.url, modelPaths) : undefined;
      return parsed && at !== undefined
         ? sourceKeyOf(parsed.source, at)
         : undefined;
   };
   interface Found {
      field: string;
      identity: string;
      sourceKey: string;
      entity: Entity;
      exists: boolean;
      depth: number;
   }
   const found: Found[] = [];
   let walkCapped = 0;
   const walked = new Set<string>();
   for (const model of args.models) {
      for (const raw of model.sourceInfos) {
         const info = raw as {
            name?: string;
            schema?: { fields?: InfoField[] };
         };
         if (!info.name) continue;
         const sourceKey = args.sourceKeyByContext.get(
            sourceContextKey(model.modelPath, info.name),
         );
         if (!sourceKey || walked.has(sourceKey)) continue;
         walked.add(sourceKey);
         const sourceName = sources.get(sourceKey)?.name ?? info.name;
         let paths = 0;
         const visit = (
            fields: readonly InfoField[],
            compiled: readonly CompiledField[] | undefined,
            path: readonly string[],
            fanout: Relationship,
            /** The id of the source the fields belong to, and the raw joins below it. */
            owner: string,
            rest: readonly string[],
         ) => {
            for (const f of fields) {
               if (f.kind === "join") {
                  if (path.length >= JOIN_WALK_MAX_HOPS) {
                     walkCapped += 1;
                     continue;
                  }
                  if (paths >= JOIN_WALK_MAX_PATHS) {
                     walkCapped += 1;
                     return;
                  }
                  paths += 1;
                  const entry = compiled?.find(
                     (c) => c.join !== undefined && activeName(c) === f.name,
                  );
                  const id = entry?.sourceID ?? entry?.extends;
                  const next =
                     path.length === 0
                        ? { joinPath: f.name, fanout: f.relationship ?? "one" }
                        : enterJoin(
                             { name: f.name, relationship: f.relationship },
                             { joinPath: path.join("."), fanout },
                          );
                  visit(
                     f.schema?.fields ?? [],
                     entry?.fields,
                     [...path, f.name],
                     next.fanout,
                     id ?? owner,
                     id ? [] : [...rest, f.name],
                  );
                  continue;
               }
               if (f.kind !== "dimension" && f.kind !== "measure") continue;
               if (path.length === 0) continue;
               const ownerKey = keyOfId(owner);
               // A field of a served source is found on that source (or was
               // folded there into another spelling of the same column).
               if (
                  rest.length === 0 &&
                  ownerKey !== undefined &&
                  sources.has(ownerKey)
               ) {
                  continue;
               }
               const joinPath = path.join(".");
               const name = `${joinPath}.${f.name}`;
               const dataType = f.type?.kind?.replace(/_type$/, "");
               found.push({
                  field: name,
                  identity: [owner, ...rest, f.kind, f.name].join("\u0000"),
                  sourceKey,
                  exists: hasNode(f.kind, sourceKey, name),
                  depth: path.length,
                  entity: {
                     id: `unserved:${sourceKey}:${name}`,
                     kind: f.kind,
                     name,
                     source: sourceName,
                     modelPath: model.modelPath,
                     doc: docText(f.annotations),
                     embedDoc: docOnlyText(f.annotations),
                     joinPath,
                     relationship: fanout,
                     ...(dataType ? { dataType } : {}),
                  } as Entity,
               });
            }
         };
         visit(
            info.schema?.fields ?? [],
            compiledSource(model.modelDef, info.name)?.fields,
            [],
            "one",
            sourceKey,
            [],
         );
      }
   }
   const byIdentity = new Map<string, Found[]>();
   for (const x of found) {
      const list = byIdentity.get(x.identity) ?? [];
      list.push(x);
      byIdentity.set(x.identity, list);
   }
   const added: Array<{ entity: Entity; sourceKey: string }> = [];
   let capped = 0;
   for (const list of byIdentity.values()) {
      list.sort(
         (a, b) =>
            a.depth - b.depth ||
            byText(a.sourceKey, b.sourceKey) ||
            byText(a.field, b.field),
      );
      let count = list.filter((x) => x.exists).length;
      for (const x of list) {
         if (x.exists) continue;
         if (count >= cap) {
            capped += 1;
            continue;
         }
         count += 1;
         added.push({ entity: x.entity, sourceKey: x.sourceKey });
      }
   }
   return { added, capped, walkCapped };
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
   // An `#(agent-hidden)` source is left out like an unserved one: no node
   // of its own, so none of its fields is ever a home card; a served
   // source's joins still reach it (joined copies, unservedCopies).
   const hidden = new Set<string>();
   for (const e of input.entities) {
      if (e.kind !== "source") continue;
      if (
         isAgentHidden(
            input.models,
            compiledSource(modelByPath.get(e.modelPath)?.modelDef, e.name),
         )
      ) {
         hidden.add(e.name);
         continue;
      }
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
      // An extension's doc is the notes it writes, never its parent's.
      const own = ownNotesOf(modelByPath.get(e.modelPath)?.modelDef, e.name);
      const node: CatalogNode = {
         key: nodeKeyOf("source", key, e.name),
         id: entityId("source", e.name, e.name),
         kind: "source",
         name: e.name,
         source: e.name,
         sourceKey: key,
         modelPaths: [e.modelPath],
         doc: own ? docOnlyText(own) : e.embedDoc,
         displayDoc: own ? docText(own) : e.doc,
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
   const addEntityNode = (e: Entity, sourceKey: string) => {
      const key = nodeKeyOf(e.kind, sourceKey, e.name);
      const existing = nodes.get(key);
      if (existing) {
         if (!existing.modelPaths.includes(e.modelPath)) {
            existing.modelPaths.push(e.modelPath);
            existing.entities.push(e);
         }
         return;
      }
      const node: CatalogNode = {
         key,
         id: entityId(e.kind, e.source ?? "", e.name),
         kind: e.kind,
         name: e.name,
         source: e.source ?? "",
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
   };
   for (const e of input.entities) {
      if (e.kind === "source" || !e.source) continue;
      const sourceKey = sourceKeyByContext.get(
         sourceContextKey(e.modelPath, e.source),
      );
      if (sourceKey) addEntityNode(e, sourceKey);
   }

   // 2b. With `unservedCopies` above 0: joined copies of fields whose own
   // source is not served, at any depth the join walk allows. The entity
   // index stops at two joins.
   const unserved =
      (input.unservedCopies ?? 0) > 0
         ? unservedCopies({
              models: input.models,
              modelPaths,
              sourceKeyByContext,
              sources,
              hasNode: (kind, sourceKey, name) =>
                 nodes.has(nodeKeyOf(kind, sourceKey, name)),
              cap: input.unservedCopies!,
           })
         : { added: [], capped: 0, walkCapped: 0 };
   for (const { entity, sourceKey } of unserved.added) {
      addEntityNode(entity, sourceKey);
   }
   for (const node of nodes.values()) {
      node.modelPaths.sort(comparePaths);
      node.entities.sort((a, b) => comparePaths(a.modelPath, b.modelPath));
   }
   for (const source of sources.values()) source.modelPaths.sort(comparePaths);

   // 3. Joins out of each source, from the entity index's topology. Walked to every
   // depth in step 4b, once step 4 has added what only the compiled model has.
   const routesMode = input.joinedCopies === "routes";
   let joins = new Map<string, JoinHop[]>();
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
   // Where each node's definition is, and each source's compiled definition,
   // for collapseCopies.
   const definitionAt = new Map<string, CompiledLocation>();
   const compiledOf = new Map<string, CompiledSource>();
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
         if (compiled) compiledOf.set(sourceKey, compiled);
         const joinKinds = new Map<string, string>();
         const joinIds = joinSourceIds(
            compiled?.fields,
            [],
            undefined,
            joinKinds,
         );
         if (routesMode) {
            // A join to a source extended in the join (`b is s extend {...}`)
            // has no target in the topology. In routes mode it is a join to
            // `s`, so `s`'s own fields are reached through it by route.
            for (const [pathText, raw] of joinIds) {
               const id = splitSourceId(raw);
               const at = id ? modelPathOfUrl(id.url, modelPaths) : undefined;
               const to =
                  id && at !== undefined
                     ? sourceKeyOf(id.source, at)
                     : undefined;
               if (!to || to === sourceKey || !sources.has(to)) continue;
               const hops = joins.get(sourceKey) ?? [];
               if (hops.some((h) => h.path.join(".") === pathText)) continue;
               const path = pathText.split(".");
               hops.push({ to, path, fanout: compiledFanout(joinKinds, path) });
               joins.set(sourceKey, hops);
               if (path.length === 1) {
                  edges.push({ kind: "joins", from: sourceKey, to, path });
               }
            }
         }
         for (const key of sources.get(sourceKey)?.fields ?? []) {
            const node = nodes.get(key)!;
            const id = splitSourceId(
               node.joinPath ? joinIds.get(node.joinPath) : undefined,
            );
            const at = id ? modelPathOfUrl(id.url, modelPaths) : undefined;
            const joined =
               id && at !== undefined ? sourceKeyOf(id.source, at) : undefined;
            if (joined && sources.has(joined) && joined !== sourceKey) {
               node.joinedSourceKey = joined;
            }
         }
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

   // 4b. Every path of the join graph, at any depth.
   const walked = walkJoins(joins);
   joins = walked.joins;

   // 4c. With joinedCopies "routes", leave out every joined copy whose field
   // has a node on the source the join reaches: the field is searched once,
   // on its own source, and reached by a route. A field of a raw-table or
   // SQL join, or one a join adds to its target, has no such node and stays.
   let joinedCopiesDropped = 0;
   if (routesMode) {
      const dropped = new Map<string, string>();
      for (const node of nodes.values()) {
         if (!node.joinPath) continue;
         if (node.kind !== "dimension" && node.kind !== "measure") continue;
         const path = node.joinPath.split(".");
         const leaf = node.name.slice(node.joinPath.length + 1);
         for (let i = path.length; i >= 1; i--) {
            const prefix = path.slice(0, i).join(".");
            const to =
               joins
                  .get(node.sourceKey)
                  ?.find((h) => h.path.join(".") === prefix)?.to ??
               (i === path.length ? node.joinedSourceKey : undefined);
            if (!to || to === node.sourceKey) continue;
            const rest = [...path.slice(i), leaf].join(".");
            // The field's own node, or the node another spelling of the same
            // column was folded into (collapseAliases).
            const own =
               nodes.get(nodeKeyOf(node.kind, to, rest)) ??
               sources
                  .get(to)
                  ?.fields.map((k) => nodes.get(k)!)
                  .find(
                     (n) => n.kind === node.kind && n.aliases?.includes(rest),
                  );
            if (own) {
               dropped.set(node.key, own.key);
               break;
            }
         }
      }
      // A copy of a copy resolves to the field itself.
      const finalOf = (key: string): string => {
         let k = key;
         for (let i = 0; i < 16 && dropped.has(k); i++) k = dropped.get(k)!;
         return k;
      };
      for (const key of dropped.keys()) {
         const node = nodes.get(key)!;
         nodes.delete(key);
         const source = sources.get(node.sourceKey);
         if (source) source.fields = source.fields.filter((k) => k !== key);
      }
      for (const node of nodes.values()) {
         node.dependsOn = [
            ...new Set(
               node.dependsOn.map((d) => (dropped.has(d) ? finalOf(d) : d)),
            ),
         ].filter((d) => d !== node.key);
         node.uses = node.uses.filter((u) => !dropped.has(u));
      }
      for (let i = edges.length - 1; i >= 0; i--) {
         const e = edges[i];
         if (
            dropped.has(e.from) ||
            (e.kind !== "depends" && dropped.has(e.to))
         ) {
            edges.splice(i, 1);
         } else if (e.kind === "depends" && dropped.has(e.to)) {
            e.to = finalOf(e.to);
         }
      }
      joinedCopiesDropped = dropped.size;
   }

   // 5. With collapseCopies, fold each inherited copy into the same field on
   // the source it was made from, but only along `extend` (or plain
   // reference) steps that keep the rows (sameRows). An extension with a
   // `where:` of its own, another base or a gate of its own holds other rows:
   // its inherited fields stay its own and are searched and returned on it.
   // A chain folds as far up as the rows stay the same, so `c is b extend
   // {dimension: ...}` folds into `b` even when `b is a extend {where: ...}`.
   let copiesFolded = 0;
   let copiesKeptApart = 0;
   if (input.collapseCopies) {
      const keyById = new Map<string, string>();
      for (const [key, compiled] of compiledOf) {
         if (compiled.sourceID) keyById.set(compiled.sourceID, key);
      }
      // The source a source's rows are the same as, one step up; undefined
      // when it was made from nothing in the catalog, or its rows differ.
      const sameRowsParent = new Map<string, string | undefined>();
      const rowsParentOf = (key: string): string | undefined => {
         if (sameRowsParent.has(key)) return sameRowsParent.get(key);
         const compiled = compiledOf.get(key);
         const parentKey = keyById.get(parentIdOf(compiled) ?? "");
         const same =
            parentKey !== undefined &&
            parentKey !== key &&
            sameRows(
               compiled,
               compiledOf.get(parentKey),
               compiled ? ownNoteTexts(input.models, compiled) : [],
            ) &&
            sources.get(key)?.gated === sources.get(parentKey)?.gated;
         const out = same ? parentKey : undefined;
         sameRowsParent.set(key, out);
         return out;
      };
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
         const bySource = new Map(list.map((n) => [n.sourceKey, n]));
         const fieldAt = definitionAt.get(list[0].key)!;
         const definer = list.find((n) => {
            const own = compiledOf.get(n.sourceKey)?.location;
            return own ? contains(own, fieldAt) : false;
         });
         for (const n of list) {
            let at = n.sourceKey;
            for (let i = 0; i < 32; i++) {
               const up = rowsParentOf(at);
               if (up === undefined || !bySource.has(up)) break;
               at = up;
            }
            let rep = bySource.get(at)!;
            if (rep === n && definer && definer !== n) {
               // A table source's extension records no parent: compare it
               // with the source whose own definition holds the field.
               const compiled = compiledOf.get(n.sourceKey);
               if (
                  compiled &&
                  sameRows(
                     compiled,
                     compiledOf.get(definer.sourceKey),
                     ownNoteTexts(input.models, compiled),
                  ) &&
                  sources.get(n.sourceKey)?.gated ===
                     sources.get(definer.sourceKey)?.gated
               ) {
                  rep = definer;
               }
            }
            if (rep === n) {
               // Inherited, with the same definition elsewhere, but its own:
               // a near-identical source holding other rows (or a definer).
               if (n !== definer) copiesKeptApart += 1;
               continue;
            }
            n.copyOf = rep.key;
            (rep.copies ??= []).push(n.key);
            copiesFolded += 1;
         }
         // The fields left searched in this group are near-identical: one
         // definition inherited onto sources whose rows differ.
         const kept = list.filter((n) => !n.copyOf);
         if (kept.length > 1) {
            const twinKey = kept.map((n) => n.key).sort()[0];
            for (const n of kept) n.twinKey = twinKey;
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
   const allPaths = [...joins.values()].flat();
   return {
      nodes,
      sources,
      edges,
      joins,
      routeDepth: routesMode ? JOIN_WALK_MAX_HOPS : ROUTE_MAX_DEPTH,
      joinedCopies: routesMode ? "routes" : "indexed",
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
         copiesKeptApart,
         hiddenSources: hidden.size,
         joinPaths: allPaths.length,
         deepestJoinPath: allPaths.reduce(
            (n, h) => Math.max(n, h.path.length),
            0,
         ),
         joinPathsCapped: walked.capped,
         joinedCopiesDropped,
         unservedCopiesAdded: unserved.added.length,
         unservedCopiesCapped: unserved.capped,
         unservedWalkCapped: unserved.walkCapped,
      },
   };
}

/**
 * The catalog input for a served package: the entity index's entities and
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
