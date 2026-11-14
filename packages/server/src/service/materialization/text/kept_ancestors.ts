// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The ancestors whose trailing `extend { … }` runs a `#(materialize)` re-root
 * keeps: an extension table's bases, and a base whose given-reading terms or
 * dimensions the build leaves out.
 *
 * The served copy replaces a source's declaration base with its stored table,
 * so whatever a base it is built from declares is gone from the served source
 * unless the re-root writes that base's blocks again. It can for a chain of
 * plain extensions in one file: `ext is mid extend { … }`, `mid is base
 * extend { … }`, `base is <anything> extend { … }`. Each ancestor's trailing
 * run is kept, root first, ahead of the source's own, over the stored
 * columns.
 *
 * One walk for both sides. The build-file generator plans from the compiled
 * model (no text), and its build copy confirms the plan against the file's
 * text; the served copy walks again with the text. Both stop where the walk
 * stops here, so a term or dimension the build leaves out because a kept
 * block re-applies it is in the served copy's kept blocks.
 *
 * Pure.
 */

import { declarationSpans, declaredBaseName } from "./declaration_text";
import { offsetOf } from "./malloy_lexer";
import type { SourceMap } from "./source_map";

/** A zero-based Malloy range, as a compiled source's `location.range` carries. */
interface KeptRange {
   start: { line: number; character: number };
   end: { line: number; character: number };
}

/** One ancestor on the kept walk, as the compiled model places it. */
export interface KeptAncestor {
   sourceID: string;
   /** Its declaration's range in the file (`<name> is <definition>`). */
   range: KeptRange;
   /**
    * Renames of it declared in the file (`b2 is base`, and renames of those).
    * The base edge reads an extension of one as an extension of the ancestor
    * itself (a rename adds nothing, so the two are alike and the edge takes
    * the one the other is built from), so the text may name one of these
    * where the walk names the ancestor.
    */
   aliases?: KeptAlias[];
}

/** A plain rename on the kept walk ({@link KeptAncestor.aliases}). */
interface KeptAlias {
   sourceID: string;
   range: KeptRange;
}

/** A {@link KeptAncestor} the file's text confirms, with its kept run. */
interface ConfirmedAncestor extends KeptAncestor {
   /**
    * Its trailing `extend { … }` run's offsets in the file, end exclusive;
    * absent when the declaration has none.
    */
   run?: { start: number; end: number };
}

/** The ancestors' walk is bounded, as every chain walk in the module is. */
const MAX_HOPS = 64;

/**
 * The ancestors of `sourceID` whose blocks its re-root can keep, nearest
 * first, from the compiled model: up `extend` and rename hops, while each
 * ancestor is declared in `url` (the source's own file). A query-shaped
 * ancestor (`x is y -> { … } extend { … }`) is kept, and the walk stops
 * there: its trailing run is over its query's output, which is what its
 * extensions' tables store. Empty when the source extends nothing in its
 * file.
 */
export function keptAncestorChain(
   map: SourceMap,
   sourceID: string,
   url: string,
): KeptAncestor[] {
   const out: KeptAncestor[] = [];
   let node = map.node(sourceID);
   for (let hops = 0; node && hops < MAX_HOPS; hops++) {
      const base = node.base;
      if (!base || base.hop === "query" || base.sourceID === undefined) break;
      const parent = map.node(base.sourceID);
      const range = locatedIn(parent?.def, url);
      if (!parent || !range) break;
      const aliases = plainRenamesOf(map, parent.sourceID, url);
      out.push({
         sourceID: parent.sourceID,
         range,
         ...(aliases.length > 0 ? { aliases } : {}),
      });
      node = parent;
   }
   return out;
}

/**
 * The renames of `sourceID` declared in `url`, transitively: each a rename hop
 * (`b2 is base`, which Malloy records as `referenceID`) to it or to one
 * already found. A rename adds nothing to its referent, so it has no block
 * to keep; the text check ({@link confirmedAncestors}) confirms each is a
 * bare rename.
 */
function plainRenamesOf(
   map: SourceMap,
   sourceID: string,
   url: string,
): KeptAlias[] {
   const out: KeptAlias[] = [];
   const reached = new Set([sourceID]);
   for (let grew = true, rounds = 0; grew && rounds < MAX_HOPS; rounds++) {
      grew = false;
      for (const node of map.nodes()) {
         const base = node.base;
         if (reached.has(node.sourceID) || base?.hop !== "rename") continue;
         if (base.sourceID === undefined || !reached.has(base.sourceID)) {
            continue;
         }
         const range = locatedIn(node.def, url);
         if (!range) continue;
         out.push({ sourceID: node.sourceID, range });
         reached.add(node.sourceID);
         grew = true;
      }
   }
   return out;
}

/**
 * The prefix of `chain` the file's `text` confirms, each with its trailing
 * run: each declaration names the next ancestor as its whole base (`<name> is
 * <ancestor> extend …`, the declared name read from its sourceID), directly
 * or through bare renames of it ({@link KeptAncestor.aliases}), and the
 * ancestor's declaration parses. `own` is the source's own declaration range.
 * The walk stops at the first step the text does not confirm.
 */
export function confirmedAncestors(
   text: string,
   own: KeptRange,
   chain: readonly KeptAncestor[],
): ConfirmedAncestor[] {
   const out: ConfirmedAncestor[] = [];
   let child = own;
   for (const ancestor of chain) {
      if (!namesAncestor(text, child, ancestor)) break;
      const from = offsetOf(text, ancestor.range.start);
      const to = offsetOf(text, ancestor.range.end);
      if (from < 0 || to < from) break;
      const spans = declarationSpans(text.slice(from, to));
      if (!spans) break;
      out.push({
         ...ancestor,
         ...(spans.extendStart !== undefined
            ? { run: { start: from + spans.extendStart, end: to } }
            : {}),
      });
      child = ancestor.range;
   }
   return out;
}

/** The name a sourceID was declared with (`name@url`). */
function declaredName(sourceID: string): string | undefined {
   const at = sourceID.indexOf("@");
   return at < 0 ? undefined : sourceID.slice(0, at);
}

/**
 * Whether the declaration at `child` names `ancestor` as its whole base, in
 * the text: by its own name, or by one of its renames, each of which the
 * text declares as a bare rename (`<alias> is <next>`, no `extend`), down to
 * the ancestor's name.
 */
function namesAncestor(
   text: string,
   child: KeptRange,
   ancestor: KeptAncestor,
): boolean {
   const aliases = new Map(
      (ancestor.aliases ?? []).map((a) => [declaredName(a.sourceID), a]),
   );
   let named = declaredBaseName(text, child);
   for (let hops = 0; named !== undefined && hops <= aliases.size; hops++) {
      if (named === declaredName(ancestor.sourceID)) return true;
      const alias = aliases.get(named);
      if (!alias) return false;
      const from = offsetOf(text, alias.range.start);
      const to = offsetOf(text, alias.range.end);
      if (from < 0 || to < from) return false;
      const spans = declarationSpans(text.slice(from, to));
      if (!spans || spans.extendStart !== undefined) return false;
      named = declaredBaseName(text, alias.range);
   }
   return false;
}

/** A compiled declaration's range when it is declared in `url`. */
function locatedIn(def: unknown, url: string): KeptRange | undefined {
   const location = (
      def as { location?: { url?: unknown; range?: KeptRange } } | undefined
   )?.location;
   return location?.url === url &&
      typeof location.range?.start?.line === "number" &&
      typeof location.range?.end?.line === "number"
      ? location.range
      : undefined;
}
